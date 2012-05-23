""" 
.. module:: DataStore
    :synopsis: Module responsible for all the data-persistence action.

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>

"""

import pycassa

import logging
import time
import json
import datetime
import multiprocessing
import collections 
import functools

from caa import SubscriptionMode, ArchivedPV
from conf import settings
import caa.utils as utils

logger = logging.getLogger('DataStore')

cfg = settings.DATASTORE
def _pool_factory():
    pool = pycassa.pool.ConnectionPool(cfg['keyspace'], cfg['servers'])
    logger.debug("Created connection pool with id '%d' for process '%s'", \
            id(pool), multiprocessing.current_process().name)
    return pool


# holds a pycassa connection pool per process
G_POOLS = collections.defaultdict(_pool_factory)

# map of process -> column families (cfname -> cfinstance)
G_CFS = collections.defaultdict(dict)

def _cf_factory(cfname):
    global G_POOLS
    procname = multiprocessing.current_process().name
    cf = pycassa.ColumnFamily(G_POOLS[procname], cfname)
    cf.read_consistency_level = cfg['consistency']['read']
    cf.write_consistency_level = cfg['consistency']['write']
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

dumps = json.dumps
loads = json.loads

#####################################################

def create_schema(server, keyspace, replication_factor, recreate):
    sm = pycassa.system_manager.SystemManager(server)
    if recreate:
        if keyspace in sm.list_keyspaces():
            sm.drop_keyspace(keyspace)

    if keyspace in sm.list_keyspaces():
        # remove all PVs from the DB
        # Used to cleanup after an execution terminates
        # without cleaning after itself (ie, after a crash)
        logger.debug("Performing cleanup")

        allpvs = ( pvname for pvname, _ in list_subscribed() )
        remove_subscriptions(allpvs)
    else: # keyspace doesn't exist. Create it
        #create it
        sm.create_keyspace(keyspace, 
                pycassa.system_manager.SIMPLE_STRATEGY, 
                {'replication_factor': str(replication_factor)}) 
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
        #       'subscribed': <bool>
        #       if 'subscribed':
        #           'modename': mode name
        #           'mode': jsoned dict repr of the mode
        #           'since': timestamp (long)
        #   }
        sm.create_column_family(keyspace, 'pvs', 
                comparator_type=pycassa.UTF8_TYPE, 
                default_validation_class=pycassa.UTF8_TYPE, 
                key_validation_class=pycassa.UTF8_TYPE, 
                compression_options={'sstable_compression': 'DeflateCompressor'})
        sm.alter_column(keyspace, 'pvs', 'subscribed', pycassa.BOOLEAN_TYPE)
        sm.alter_column(keyspace, 'pvs', 'since', pycassa.LONG_TYPE)

        sm.create_index(keyspace, 'pvs', 'subscribed', pycassa.BOOLEAN_TYPE, 
                index_name='subscription_index')
        sm.create_index(keyspace, 'pvs', 'modename', pycassa.UTF8_TYPE, 
                index_name='mode_index')

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
                key_validation_class=pycassa.TIME_UUID_TYPE,
                compression_options={'sstable_compression': 'DeflateCompressor'})

     
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
    d = dict( (k, dumps(v)) for k,v in extra.iteritems() )

    d.update( {'pvname': dumps(pvname), 
               'value': dumps(value),
               'archived_at': dumps(ts_readable),
               'archived_at_ts': dumps(ts_ms),
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

def add_subscriptions(pvnames, modes):
    if len(pvnames) != len(modes):
        raise ValueError("Argument sizes don't match")

    b = _cf('pvs').batch()

    for pvname, mode in zip(pvnames, modes):
        modename = mode['mode']
        mode_jsoned = dumps(mode)

        d = {'modename': modename, 'mode': mode_jsoned, 
             'since': _get_timestamp_ms(), 'subscribed': True} 
        logger.debug("Adding subscription to '%s' with '%s'", pvname, d)
        b.insert(pvname, d)
    b.send()

def remove_subscriptions(pvnames):
    """ Sets all PVs in `pvnames` to the single set of columns `cols` """
    b = _cf('pvs').batch()
    newcols = {'subscribed': False, 'since': _get_timestamp_ms()}
    for pvname in pvnames:
        logger.info("Updating PV '%s' to '%s'", pvname, newcols)
        b.insert(pvname, newcols)
    b.send()

############### READERS ###########################

def list_archived(include_subscribed):
    if include_subscribed: # ie, all
        rowsgen = _cf('pvs').get_range()
    else: # return only archived but not subscribed PVs
        expr = pycassa.index.create_index_expression('subscribed', False)
        cls = pycassa.index.create_index_clause([expr], count=2**31)
        rowsgen = _cf('pvs').get_indexed_slices(cls)
    return ((k, ArchivedPV(k, **v)) for k,v in rowsgen)

def list_subscribed():
    expr = pycassa.index.create_index_expression('subscribed', 'true')
    cls = pycassa.index.create_index_clause([expr], count=2**31)
    rowsgen = _cf('pvs').get_indexed_slices(cls)
    return ((k, ArchivedPV(k, **v)) for k,v in rowsgen)

def list_by_mode(modename):
    if not SubscriptionMode.is_registered(modename):
        raise ValueError("'%s' is not a valid mode" % modename)

    expr = pycassa.index.create_index_expression('modename', modename)
    cls = pycassa.index.create_index_clause([expr], count=2**31)
    rowsgen = _cf('pvs').get_indexed_slices(cls)
    return ((k, ArchivedPV(k, **v)) for k,v in rowsgen)

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
        subscribed = cols['subscribed']
        since = cols['since']
        if subscribed:
            modedict = loads(cols['mode'])
            mode = SubscriptionMode.parse(modedict)
            return ArchivedPV(pvname, subscribed, mode, since)
        return ArchivedPV(pvname, subscribed, since=since)

    pvrows = _cf('pvs').multiget(pvnames)
    pvs = dict( (pvname, _parse(pvname, cols)) \
            for pvname, cols in pvrows.iteritems() )
    return pvs 

def read_values(pvname, fields, limit, ini, end, reverse):
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
        # returns a list of dicts: [{ PV stuff }, ..., {PV stuff}]

        # get all the update_ids
        update_ids = timeline.values()
        # now retrieve those with a multiget
        updates_cf = _cf('updates')
        updates = updates_cf.multiget(update_ids) # updates is a dict { up_id: { pv: ..., value: ...} }
        rows = []
        for pv_data in updates.itervalues():
            out_data = {'archived_at_ts': loads(pv_data['archived_at_ts']),
                        'archived_at': loads(pv_data['archived_at'])}
            for k in (fields or pv_data.keys()):
                if k not in pv_data:
                    logger.warning("Field '%s' not found in PV data '%s'", k, pv_data)
                out_data[k] = loads(pv_data.get(k) or 'null') 

            rows.append(out_data)
        return rows

    ts_ini = int(ini) if ini else ''
    ts_end = int(end) if end else ''

    try: 
        timeline = _cf('update_timeline').get(pvname, \
                column_count=limit, column_reversed=reverse,\
                column_start=ts_ini, column_finish=ts_end)
        rows = _join_timeline_with_updates(timeline)
    except pycassa.NotFoundException:
        rows = []

    return rows

