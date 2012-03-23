"""
.. module:: DataStore
    :synopsis: Module responsible for all the data-persistence action.

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>

"""

import pycassa
from pycassa.cassandra.ttypes import NotFoundException

import logging
import time
import itertools
import json
import datetime
import sys
import fnmatch 
import multiprocessing
from collections import defaultdict

from caa import SubscriptionMode, ArchivedPV
from caa.conf import settings
import caa.utils as utils

logger = logging.getLogger('DataStore')

def _pool_factory():
    cfg = settings.DATASTORE
    pool = pycassa.pool.ConnectionPool(cfg['keyspace'], cfg['servers'])
    logger.debug("Created connection pool with id '%d' for process '%s'", \
            id(pool), multiprocessing.current_process().name)
    return pool


# holds a pycassa connection pool per process
G_POOLS = defaultdict(_pool_factory)

# map of process -> column families (cfname -> cfinstance)
G_CFS = defaultdict(dict)

def _cf_factory(cfname):
    global G_POOLS
    procname = multiprocessing.current_process().name
    cf = pycassa.ColumnFamily(G_POOLS[procname], cfname)
    return cf

def _cf(cfname):
    global G_CFS
    procname = multiprocessing.current_process().name
    cfs_for_proc = G_CFS[procname]
    if cfname not in cfs_for_proc:
        cfs_for_proc[cfname] = _cf_factory(cfname)
    return cfs_for_proc[cfname]

def _get_timestamp_ms():
    ts = int(time.time() * 1e6)
    return ts

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

    if keyspace in sm.list_keyspaces():
        # remove all PVs from the DB
        # Used to cleanup after an execution terminates
        # without cleaning after itself (ie, after a crash)
        logger.debug("Performing cleanup")

        allpvs = list_pvs()
        remove_pvs(allpvs)
    else: # keyspace doesn't exist. Create it
        #create it
        sm.create_keyspace(keyspace, 
                pycassa.system_manager.SIMPLE_STRATEGY, {'replication_factor': '1'})
        logger.debug("Keyspace %s created", keyspace)


        # statuses: 
        #   pvname: {
        #       ts: boolean
        #   }
        #
        # for each pv, the boolean signals the new connection status acquired at "ts"
        sm.create_column_family(keyspace, 'statuses', 
                comparator_type=pycassa.LONG_TYPE,      # column name type
                default_validation_class=pycassa.BOOLEAN_TYPE, # column value type
                key_validation_class=pycassa.UTF8_TYPE) # row type

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
        #      'pvname': pvname,
        #      'value': value, 
        #      ...
        #   }
        # }
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
        create_schema(server, keyspace, ks_props['strategy_options']['replication_factor'])


############### WRITERS ###########################

def save_update(update_id, pvname, value, **extra):
    ts_ms = _get_timestamp_ms()
    ts_readable = datetime.datetime.fromtimestamp(time.time()).isoformat(' ')

    d = dict( (k, json.dumps(v)) for k,v in extra.iteritems() )
    d.update( {'pvname': json.dumps(pvname), 
               'value': json.dumps(value),
               'archived_at': json.dumps(ts_readable),
               'archived_at_ts': json.dumps(ts_ms),
               }
            )
    logger.debug("Saving update '(%s, %s)'", \
            update_id, d)

    _cf('updates').insert(update_id, d)

    _cf('update_timeline').insert(pvname, {ts_ms: update_id})


def save_conn_status(pvname, connected):
    ts = _get_timestamp_ms() 
    d = {ts: connected}
    logger.debug("Updating connection status for '%s' to '%s'", pvname, connected)
    _cf('statuses').insert(pvname, d)

def save_pvs(pvnames, modes):
    """ 
        Register the given PVs with the system. 

        :param pvnames: Collection of PV names.
        :param modes: Collection of the corresponding subscription modes, 
        instances of :class:`SubscriptionMode` 
    """
    if len(pvnames) != len(modes):
        raise ValueError("Argument sizes don't match")

    b = _cf('pvs').batch()

    for pvname,mode in zip(pvnames, modes):
        ts = _get_timestamp_ms() 
        d = {'since': ts} 

        # for each of the mode values, ensure it's 
        # in str form 
        mode_jsoned = dict( (k, json.dumps(v)) for k,v in mode.iteritems() ) 
        d.update(mode_jsoned)

        logger.debug("Saving pv subscription '(%s, %s)'", pvname, d)

        b.insert(pvname, d)
    b.send()



###### REMOVERS ######################
def remove_pvs(pvnames):
    if not utils.is_non_str_iterable(pvnames):
        raise TypeError("The argument isn't a collection of PV names")
    b = _cf('pvs').batch()
    for pvname in pvnames:
        logger.info("Removing PV '%s'", pvname)
        b.remove(pvname)
    b.send()

############### READERS ###########################

#XXX: right now, get_indexed_slices and get_range return all columns
# there doesnt seem to be a way to not return any column. Instead of
# picking an arbitrary column 
def list_pvs(namepattern, modename):
    if modename:
        jsoned_mode_name = json.dumps(modename)
        expr = pycassa.index.create_index_expression('mode', jsoned_mode_name)
        cls = pycassa.index.create_index_clause([expr], count=2**31)
        rowsgen = _cf('pvs').get_indexed_slices(cls)
    else:
        rowsgen = _cf('pvs').get_range()

    # filter by name
    if namepattern:
        return ( pvname for pvname, _ in rowsgen if fnmatch.fnmatch(pvname, namepattern) )
    else:
        return ( pvname for pvname, _ in rowsgen )

def read_status(pvname, limit, ini, end):
    ts_ini = ini and time.mktime(ini.timetuple()) * 1e6 or ''
    ts_end = end and time.mktime(end.timetuple()) * 1e6 or ''
    
    try:
        statuses = _cf('statuses').get(pvname, \
                column_count=limit, column_reversed=True, \
                column_start=ts_end, column_finish=ts_ini)
        res = [(ts, status) for ts,status in statuses.iteritems()]
    except pycassa.NotFoundException:
        res = []

    return res

def read_pvs(pvnames):
    if not utils.is_non_str_iterable(pvnames):
        raise TypeError("The argument isn't a collection of PV names")

    def _parse(pvname, cols):
        since = cols.pop('since')
        # cols's remaining values (mode as a dict) are json'd 
        mode_dict = dict( (k, json.loads(v)) for k,v in cols.iteritems() )
        mode = SubscriptionMode.parse(mode_dict)
        return ArchivedPV(pvname, mode, since)

    pvrows = _cf('pvs').multiget(pvnames)
    pvs = dict( (pvname, _parse(pvname, cols)) \
            for pvname, cols in pvrows.iteritems() )
    return pvs 

def read_values(pvname, fields, limit, ini, end):
    """ Returns a list of at most `limit` elements (dicts) for `pvname` from
        `ini` until `end`.

        :param pvname: the PV
        :param fields: which PV fields to read (value, type, etc.). If empty, read all.

        :param limit: number of entries to return
        :param ini: `datetime` instance. First returned item will be as or more recent than it.
        :param end: `datetime` instance. Last returned item will not be more recent than it.
    """
    def _join_timeline_with_updates(timeline):
        # timeline is a dict of the form (timestamp, update_id)...
        # join with UPDATES based on update_id

        # get all the update_ids
        update_ids = timeline.values()
        # now retrieve those with a multiget
        updates = _cf('updates').multiget(update_ids) # updates is a dict { up_id: { pv: ..., value: ...} }
        res = []
        for pv_data in updates.itervalues():
            out_data = {}
            for k in (fields or pv_data.keys()):
                out_data[k] = json.loads(pv_data.get(k)) # if field not in pv, it'll be null
            res.append(out_data)
        return res

    # turn datetime's into timestamps (secs from epoch). 
    # And then into ms, as that's how the timestamps are represented
    # in the cf
    ts_ini = ini and time.mktime(ini.timetuple()) * 1e6 or ''
    ts_end = end and time.mktime(end.timetuple()) * 1e6 or ''

    try:
        timeline = _cf('update_timeline').get(pvname, \
                column_count=limit, column_reversed=True,\
                column_start=ts_end, column_finish=ts_ini)
    except pycassa.NotFoundException:
        return []
    res = _join_timeline_with_updates(timeline)
    return res

##########################################################


