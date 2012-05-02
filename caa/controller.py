from __future__ import print_function

import json
import logging
import uuid
import time 
import datetime 
import itertools
#try:
#    from collections import namedtuple 
#except ImportError:
#    from caa.utils.namedtuple import namedtuple

from caa import SubscriptionMode, datastore
from tasks import Task, TimersPool, WorkersPool
from conf import settings


logger = logging.getLogger('controller')

#######################################################

workers = WorkersPool(settings.CONTROLLER['num_workers'])
timers = TimersPool(workers, settings.CONTROLLER['num_timers'])

def subscribe(pvname, mode):
    res = msubscribe( (pvname, ), (mode, ))
    return res[0] if res else None

def msubscribe(pvnames, modes):
    current_subs = tuple(i[0] for i in list_subscribed())
    newpvs = []
    newmodes = []

    futures = []
    for pvname, mode in zip(pvnames, modes):
        if pvname not in current_subs:
            task = Task(pvname, epics_subscribe, pvname, mode)
            futures.append(workers.request(task))
            if mode.name == SubscriptionMode.Scan.name:
                # in addition, add it to the timer so that it gets scanned
                # periodically
                periodic_task = Task(pvname, epics_periodic, pvname, mode.period)
                timers.schedule(periodic_task, mode.period)
            newpvs.append(pvname)
            newmodes.append(mode)
        else:
            logger.warn("Already subscribed to '%s', ignoring", pvname)

    datastore.add_subscriptions(newpvs, newmodes)
    return futures 


def unsubscribe(pvname):
    res = munsubscribe( (pvname, ) )
    return res[0] if res else None

def munsubscribe(pvnames):
    futures = []
    pvs_dict = get_apvs(pvnames)
    datastore.remove_subscriptions(pvs_dict.iterkeys())
    for pvname in pvnames: # to maintain the order
        apv = pvs_dict[pvname]
        if apv['subscribed']:
            task = Task(pvname, epics_unsubscribe, pvname)

            if apv.mode.mode == SubscriptionMode.Scan.name:
                # cancel timers 
                timers.remove_task(task.name)

            futures.append(workers.request(task))
        else:
            logger.warn("Tried to unsubscribe from non-subscribed PV '%s'. Ignored",
                    pvname)
    return futures 

def get_statuses(pvname, limit=10):
    """ Returns the ``limit`` latest connection status for the given PV.
    
        The returned list is sorted, starting with the most recent.
    """
    sts = datastore.read_status(pvname, limit=limit, ini=None, end=None)
    # sts is a list of pairs (timestamp, connected)
    res = [{'pvname': pvname, 'timestamp': st[0], 'connected': st[1]} for st in sts] 
    return res

def get_values(pvname, fields=[], limit=100, from_date=None, to_date=None):
    """ Returns latest archived data for the PV as a list with at most ``limit`` elements """
    return datastore.read_values(pvname, fields, limit, from_date, to_date)

def get_apv(pvname):
    return get_apvs([pvname]).get(pvname)

def get_apvs(pvnames):
    return datastore.read_pvs(pvnames)

def list_archived(include_subscribed=True):
    return datastore.list_archived(include_subscribed)

def list_subscribed():
    return datastore.list_subscribed()

def list_by_mode(modename):
    return datastore.list_by_mode(modename)


############################

def load_config(configstr):
    """ Restore the state defined by the config.
    
        Returns a list with the receipts of the restored subscriptions.
    """
    
    receipts = []
    without_comments = [ line for line in configstr.splitlines() if not line.lstrip().startswith('#') ]
    jsondata = ''.join(without_comments)
    decoded_l = json.loads(jsondata)
    for item in decoded_l:
        mode_dict = item['mode']
        name = item['name']
        mode = SubscriptionMode.parse(mode_dict)
        receipt = subscribe(name, mode)
        receipts.append(receipt)
    return receipts

def save_config():
    """ Save current subscription state. """
    from contextlib import closing
    import cStringIO 
    from getpass import getuser
    from platform import uname
    
    with closing(cStringIO.StringIO()) as out:
        # get a list of the ArchivedPV values 
        raw = [ {'name': apv.name, 'mode': apv.mode} \
                for _, apv in list_subscribed() ]
        datestr = datetime.datetime.now().ctime()
        out.write("# Generated on %s by '%s' on '%s'\n" % (datestr, getuser(), uname()[1] ) )
        out.write(json.dumps(raw, indent=4))

        return out.getvalue()

def get_settings():
    return settings

def shutdown():
    logger.info("Unsubscribing from all subscribed PVs...")

    all_pvs = (pvname for pvname, _ in list_subscribed())
    munsubscribe(all_pvs)

    if workers.running:
        workers.stop()
    if timers.running:
        timers.stop()


    logger.info("Shutdown completed")

def initialize(replication_factor=2, recreate=False):
    datastore.create_schema(settings.DATASTORE['servers'][0], settings.DATASTORE['keyspace'],\
            replication_factor, recreate)


##################### TASKS #######################
def epics_subscribe(state, pvname, mode):
    """ Function to be run by the worker in order to subscribe """
    import epics
    logger.info("Subscribing to '%s' with mode '%s'", pvname, mode)
    conn_timeout = settings.CONTROLLER['epics_connection_timeout']

    sub_mask = None
    cb = None
    if mode.name == SubscriptionMode.Monitor.name:
        cb = subscription_cb
        # DBE_VALUE--when the channel's value changes by more than MDEL.
        # DBE_LOG--when the channel's value changes by more than ADEL.
        # DBE_ALARM--when the channel's alarm state changes.
        sub_mask = epics.dbr.DBE_ALARM | epics.dbr.DBE_LOG | epics.dbr.DBE_VALUE

    pv = epics.PV(pvname, 
            callback=cb, 
            connection_callback=connection_cb,
            connection_timeout=conn_timeout,
            auto_monitor=sub_mask
            )
    pv.mode = mode
    pv.wait_for_connection()

    state['pv'] = pv
    return True # needed to signal that the subscription req has been completed.
                # for example, to be able to sync on controller.get_result 


def epics_unsubscribe(state, pvname):
    """ Function to be run by the worker in order to unsubscribe """
    logger.info("Unsubscribing from %s", pvname)
    pv = state['pv']
    pv.disconnect()
    del state['pv']

    return True # needed to signal that the subscription req has been completed.
                # for example, to be able to sync on controller.get_result 


def epics_periodic(state, pvname, period):
    """ Invoked every time a period for a scanned PV is due """
    logger.debug("Periodic scan for PV '%s' with period %f secs", pvname, period)
    
    # get the pv's value
    pv = state.get('pv')
    if pv:
        if pv.connected:
            #generate the id for the update
            update_id = uuid.uuid1()
            data = _gather_pv_data(pv)
            datastore.save_update(update_id, **data) 
        else: # not connected
            logger.warn("Non-connected PV '%s'. Saving with 'null' value", pvname)
            # save PV data with PV value as None/null
            update_id = uuid.uuid1()
            datastore.save_update(update_id, timestamp=time.time(), pvname=pvname, value=None) 
    else:
        logger.error("Missing data in 'state' for '%s'", pvname)


##################### CALLBACKS #######################
def subscription_cb(**kw):
    """ Internally called when a new value arrives for a subscribed PV. """
    logger.debug("PV callback invoked: %(pvname)s changed to value %(value)s at %(timestamp)s" % kw)

    pv = kw['cb_info'][1]
    pvname = kw['pvname']
    value = kw['value']

    max_f = pv.mode.max_freq 
    last_arch_time = getattr(pv, 'last_arch_time', None)
    now = pv.timestamp
    # archive iff
    # 1) we are below max frequency
    # AND
    # 2) the change in value is >= delta
    if last_arch_time:
        freq = 1/(now - last_arch_time)
        if max_f and freq > max_f: # if max_f is false, it always passes
            #logger.warn("Values for '%s' arriving faster than %f Hz: %f Hz", pvname, max_f, freq)
            return
        
        # here, we've passed the frequency test
        last_value = getattr(pv, 'last_value', None)
        if last_value:
            d = abs( last_value - value )
            if d < pv.mode.delta:
                logger.debug("Value '%r' below delta (%r) for '%s'", value, pv.mode.delta, pvname)
                return
    
    # if there's no previous recorded "frequency", archive
    # likewise for value

    # if we've made it this far, archive
    setattr(pv, 'last_arch_time', now)
    setattr(pv, 'last_value', value)

    #generate the id for the update
    update_id = uuid.uuid1()
    data = _gather_pv_data(pv)

    datastore.save_update(update_id, **data) 

def connection_cb(pvname, conn, **kw):
    logger.debug("PV '%s' connected: %s", pvname, conn)
    
    datastore.save_conn_status(pvname, conn)
    if not conn:
        # save PV data with PV value as None/null
        update_id = uuid.uuid1()
        datastore.save_update(update_id, timestamp=time.time(), pvname=pvname, value=None) 

def _gather_pv_data(pv):
    to_consider = itertools.chain.from_iterable(
            settings.ARCHIVER['pvfields'].values())
    
    data = dict( (k, getattr(pv, k)) for k in to_consider)
    #dt = datetime.datetime.fromtimestamp(data['timestamp'])
    #data['datetime'] = dt.isoformat(' ')
    return data

##############################################################

