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
import itertools
import json
import sys
import multiprocessing
from collections import defaultdict

from caa import SubscriptionMode, ArchivedPV
import caa.config as config

logger = logging.getLogger('DataStore')

def _pool_factory():
    cfg = config.DATASTORE
    pool = pycassa.pool.ConnectionPool(cfg['keyspace'], cfg['servers'] )
    logger.debug("Created connection pool with id '%d' for process '%s'", \
            id(pool), multiprocessing.current_process().name)
    return pool

G_POOLS = defaultdict(_pool_factory)
def _cf(cfname):
    global G_POOLS
    mod = sys.modules[__name__]
    priv_name = ('_' + cfname.upper())
    if priv_name not in mod.__dict__:
        mod.__dict__[priv_name] = pycassa.ColumnFamily(G_POOLS[multiprocessing.current_process().name], cfname)
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
        sm.create_keyspace(keyspace, 
                pycassa.system_manager.SIMPLE_STRATEGY, {'replication_factor': '1'})
        logger.debug("Keyspace %s created", keyspace)


        # pvs: {
        #   pvname: {
        #       'mode': mode name
        #       '<mode_arg_1>': serialized value 
        #       '<mode_arg_2>': serialized value
        #       etc
        #       'since': timestamp (long)
        #   }
        sm.create_column_family(keyspace, 'pvs', 
                comparator_type=pycassa.UTF8_TYPE, 
                default_validation_class=pycassa.UTF8_TYPE, 
                key_validation_class=pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'pvs', 'mode', pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'pvs', 'since', pycassa.LONG_TYPE)
        # We create a secondary index on "mode" to be able to retrieve
        # PVs by their subscription mode
        sm.create_index(keyspace, 'pvs', 'mode', pycassa.UTF8_TYPE, index_name='mode_index')

        #########################
        ########### UPDATES
        #########################
        # update_timeline: {
        #   pvname: {
        #      ts: update_id
        #   }
        # }
        sm.create_column_family(keyspace, 'update_timeline', 
                comparator_type=pycassa.LONG_TYPE, 
                default_validation_class=pycassa.TIME_UUID_TYPE,
                key_validation_class=pycassa.UTF8_TYPE)

        # updates: {
        #   'update_id':{ 
        #      'pv': pvname,
        #      'value': value, 
        #      ...
        #   }
        # }
        sm.create_column_family(keyspace, 'updates', 
                comparator_type=pycassa.UTF8_TYPE, 
                default_validation_class=pycassa.UTF8_TYPE, 
                key_validation_class=pycassa.TIME_UUID_TYPE)


        #########################
        ########### STATUS
        #########################
        # status_timeline: {
        #   pvname : {
        #     ts: {
        #       'pv': pvname,
        #       'connected': bool
        #       }
        #   }
        # }
        sm.create_column_family(keyspace, 'status_timeline', 
                super=True,
                comparator_type=pycassa.LONG_TYPE,  #timestamp
                subcomparator_type=pycassa.UTF8_TYPE,
                key_validation_class=pycassa.UTF8_TYPE)
        sm.alter_column(keyspace, 'status_timeline', 'connected', pycassa.BOOLEAN_TYPE)

     
def reset_schema(server, keyspace, drop=False):
    """ Wipes the whole store clean.
    
        :param boolean drop: if ``True``, the keyspace will also be dropped.
    """
    sm = pycassa.system_manager.SystemManager(server)
    ks_props = sm.get_keyspace_properties(keyspace)

    sm.drop_keyspace(keyspace)
    if not drop: #recreate
        create_schema(server, keyspace, ks_props['strategy_options']['replication_factor'])


############### WRITERS ###########################

def save_update(update_id, pvname, value, **extra):
    d = dict( (k, json.dumps(v)) for k,v in extra.iteritems() )
    d.update( {'pv': json.dumps(pvname), 
               'value': json.dumps(value),
               }
            )
    logger.debug("Saving update '(%s, %s)'", \
            update_id, d)

    ts = int(time.time() * 1e6) 
    _cf('updates').insert(update_id, d)

    _cf('update_timeline').insert(pvname, {ts: update_id})

def save_status(status_id, pvname, connected):
    d = {'pv': pvname, 'connected': connected}
    ttl = config.DATASTORE['status_ttl']
    ts = int(time.time() * 1e6)
    logger.debug("Saving status '(%s, %s)' for %d seconds", \
            pvname, connected, ttl)

    _cf('status_timeline').insert(pvname, {ts: d}, ttl = ttl)

def save_pv(pvname, mode):
    """ 
        Register the given PV with the system. 

        :param pvname: Name of the PV being persisted
        :param mode: An instance of one of the inner classes of 
        :class:`SubscriptionMode` 
    """
    ts = int(time.time() * 1e6) 
    d = {'since': ts} 

    mode_dict = mode.as_dict()
    # for each of the mode_dict's values, ensure it's 
    # in str form 
    for k,v in mode_dict.iteritems():
        mode_dict[k] = json.dumps(v)
    d.update(mode_dict)

    logger.debug("Saving pv subscription '(%s, %s)'", \
            pvname, d)

    _cf('pvs').insert(pvname, d)


###### REMOVERS ######################
def remove_pv(pvname):
    logger.debug("Removing pv subscription '%s'", pvname)
    _cf('pvs').remove(pvname) 


############### READERS ###########################
def read_pv(pvname):
    try:
        cols = _cf('pvs').get(pvname)
        
        since = cols.pop('since')
        
        # cols's remaining values (mode as a dict) are json'd 
        mode_dict = dict( (k, json.loads(v)) for k,v in cols.iteritems() )
        
        mode = SubscriptionMode.parse(mode_dict)

        apv = ArchivedPV(pvname, mode, since)
    except NotFoundException:
        return None
    return apv
def list_pvs(modes=SubscriptionMode.available_modes):
    """ Returns an iterator over the list of PV's matching the modes in 
        the ``modes`` list. 
        If ``modes`` isn't specified, return all known PV's
    """
    res = iter([])
    for mode in modes:
        jsoned_mode_name = json.dumps(mode.name)
        expr = pycassa.index.create_index_expression('mode', jsoned_mode_name)
        cls = pycassa.index.create_index_clause([expr], count=2**31)
        res = itertools.chain(res, _cf('pvs').get_indexed_slices(cls))
    def APVGen():
        for pv in res:
            pvname, pvinfo = pv
            since = pvinfo.pop('since') 
            
            mode_dict = dict( (k, json.loads(v)) for k,v in pvinfo.iteritems() )
            mode = SubscriptionMode.parse(mode_dict)
            apv = ArchivedPV(pvname, mode, since)
            yield apv
    return APVGen()


def read_values(pvname, limit, ini, end):
    """ Returns a list of at most `limit` elements (dicts) for `pvname` from
        `ini` until `end`.

        :param pvname: the PV
        :param limit: number of entries to return
        :param ini: `datetime` instance. First returned item will be as or more recent than it.
        :param end: `datetime` instance. Last returned item will not be more recent than it.
    """
    # turn datetime's into timestamps (secs from epoch). 
    # And then into ms, as that's how the timestamps are represented
    # in the cf
    ts_ini = ini and time.mktime(ini.timetuple()) * 1e6 or ''
    ts_end = end and time.mktime(end.timetuple()) * 1e6 or ''

    try:
        timeline = _cf('update_timeline').get(pvname, \
                column_count=limit, \
                column_start=ts_ini, column_finish=ts_end)
    except pycassa.NotFoundException:
        return []
    res = _join_timeline_with_updates(timeline)
    return res

def _join_timeline_with_updates(timeline):
    # timeline is a dict of the form (timestamp, update_id)...
    # join with UPDATES based on update_id

    # get all the update_ids
    update_ids = timeline.values()
    # now retrieve those with a multiget
    updates = _cf('updates').multiget(update_ids) # updates is a dict { up_id: { pv: ..., value: ...} }
    res = []
    for pv_data in updates.itervalues():
        for k in pv_data.keys():
            pv_data[k] = json.loads(pv_data[k])
        res.append(pv_data)
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


