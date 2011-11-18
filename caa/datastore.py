"""
.. module:: DataStore
    :synopsis: Module responsible for all the data-persistence action.

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>

"""

import pycassa
from pycassa.cassandra.ttypes import NotFoundException

try:
    from collections import namedtuple 
except ImportError:
    from caa.utils.namedtuple import namedtuple
import logging
import time

from caa import SubscriptionMode, ArchivedPV
import caa.config as config

logger = logging.getLogger('DataStore')

def _pool_factory():
    cfg = config.DATASTORE
    pool = pycassa.pool.ConnectionPool(cfg['keyspace'], cfg['servers'])
    return lambda: pool

def _cf(cfname):
    import sys
    mod = sys.modules[__name__]
    priv_name = ('_' + cfname.upper())
    pool = _pool_factory()()
    if priv_name not in mod.__dict__:
        mod.__dict__[priv_name] = pycassa.ColumnFamily(pool, cfname)
    return mod.__dict__[priv_name]

class DataStoreError(Exception):
    pass

class NotFound(DataStoreError):
    pass

#####################################################

def create_schema(server, keyspace, replication_factor=1, recreate=False):
    sm = pycassa.system_manager.SystemManager(server)
    if recreate:
        if keyspace in sm.list_keyspaces():
            sm.drop_keyspace(keyspace)

    if keyspace not in sm.list_keyspaces():
        #create it
        sm.create_keyspace(keyspace, replication_factor)
        logger.debug("Keyspace %s created", keyspace)

        sm.create_column_family(keyspace, 'pvs', 
                comparator_type=pycassa.UTF8_TYPE, 
                default_validation_class=pycassa.UTF8_TYPE, 
                key_validation_class=pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'pvs', 'mode', pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'pvs', 'monitor_delta', pycassa.FLOAT_TYPE)
        sm.alter_column(keyspace, 'pvs', 'scan_period', pycassa.FLOAT_TYPE)
        sm.alter_column(keyspace, 'pvs', 'since', pycassa.LONG_TYPE)

        sm.create_column_family(keyspace, 'update_timeline', 
                comparator_type=pycassa.LONG_TYPE, 
                default_validation_class=pycassa.TIME_UUID_TYPE,
                key_validation_class=pycassa.UTF8_TYPE)

        sm.create_column_family(keyspace, 'status_timeline', 
                super=True,
                comparator_type=pycassa.LONG_TYPE,  #timestamp
                subcomparator_type=pycassa.UTF8_TYPE,
                key_validation_class=pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'status_timeline', 'connected', pycassa.BOOLEAN_TYPE)

        sm.create_column_family(keyspace, 'updates', 
                comparator_type=pycassa.UTF8_TYPE, 
                default_validation_class=pycassa.UTF8_TYPE, 
                key_validation_class=pycassa.TIME_UUID_TYPE)
     
def reset_schema(server, keyspace, drop=False):
    """ Wipes the whole store clean.
    
        :param boolean drop: if ``True``, the keyspace will also be dropped.
    """
    sm = pycassa.system_manager.SystemManager(server)
    ks_props = sm.get_keyspace_properties(keyspace)

    sm.drop_keyspace(keyspace)
    if not drop: #recreate
        create_schema(server, keyspace, ks_props['replication_factor'])


############### WRITERS ###########################

def save_update(update_id, pvname, value, value_type):
    d = {'pv': pvname, 
         'value': str(value),
         'value_type': value_type,
        }

    logger.debug("Saving update '(%s, %s)'", \
            update_id, d)

    ts = int(time.time() * 1e6) 
    # updates: {
    #   'update_id':{ 
    #      'pv': pvname,
    #      'value': value, 
    #      ...
    #   }
    # }
    _cf('updates').insert(update_id, d)

    # update_timeline: {
    #   pvname: {
    #      ts: update_id
    #   }
    # }
    _cf('update_timeline').insert(pvname, {ts: update_id})

def save_status(status_id, pvname, connected):
    d = {'pv': pvname, 'connected': connected}
    ttl = config.DATASTORE['status_ttl']
    ts = int(time.time() * 1e6)
    logger.debug("Saving status '(%s, %s)' for %d seconds", \
            pvname, connected, ttl)

    # status_timeline: {
    #   pvname : {
    #     ts: {
    #       'pv': pvname,
    #       'connected': bool
    #       }
    #   }
    # }
    _cf('status_timeline').insert(pvname, {ts: d}, ttl = ttl)

def save_pv(archived_pv):
    ts = int(time.time() * 1e6) 
    d = {'mode': archived_pv.mode, 'since': ts}
    if archived_pv.mode == SubscriptionMode.MONITOR:
        d['monitor_delta'] = archived_pv.monitor_delta
    else:
        d['scan_period'] = archived_pv.scan_period

    logger.debug("Saving pv subscription '(%s, %s)'", \
            archived_pv.name, d)

    # pvs: {
    #   pvname: {
    #       'mode': ... (str)
    #       'monitor_delta' | 'scan_period': (float)
    #       'since': timestamp (long)
    #   }
    _cf('pvs').insert(archived_pv.name, d)

def remove_pv(archived_pv):
    logger.debug("Removing pv subscription '%s'", archived_pv.name)
    _cf('pvs').remove(archived_pv.name) 


############### READERS ###########################

def read_pv(pvname):
    try:
        cols = _cf('pvs').get(pvname)
        apv = ArchivedPV(pvname, **cols)
        print(apv)
    except NotFoundException:
        return None
    return apv


def read_for_dates(pvname, ini, end):
    pass

def read_latest_values(pvname, limit):
    """ Returns a list of at most ``limit`` elements containing the 
        latest values for ``pvname``
    """
    try:
        timeline = _cf('update_timeline').get(pvname, column_count=limit)
    except pycassa.NotFoundException:
        return []
    # timeline is a dict of the form (timestamp, update_id)...
    # join with UPDATES based on update_id

    # get all the update_ids
    update_ids = timeline.values()
    # now retrieve those with a multiget
    updates = _cf('updates').multiget(update_ids) # updates is a dict { up_id: { pv: ..., value: ...} }

    res = [ pv_data for pv_data in updates.itervalues() ]
    return res

def read_latest_status(pvnames):
    tl = _cf('status_timeline').multiget(pvnames, column_count=1, column_reversed=True)
    # tl is a dict of the form {pvname: {timestamp: {'pv': pvname, 'connected': bool}}}
    # OR an empty dict if no values for a given pv
    res = {}
    for pvname in pvnames:
        if pvname in tl:
            info = tl[pvname]
            for ts, data in info.iteritems():
                connected = data['connected']
                res[pvname] = {'timestamp': ts, 'connected': connected} 
        else:
            res[pvname] = {}

    return res


