from __future__ import print_function

import json
import logging
import uuid
import time 
import fnmatch
import datetime 
from caa import ArchivedPV, SubscriptionMode

import datastore
import caa.config as config
from tasks import Task, TimersPool, WorkersPool

import pycassa

logger = logging.getLogger('controller')

#######################################################

workers = WorkersPool(config.CONTROLLER['num_workers'])
timers = TimersPool(workers, config.CONTROLLER['num_timers'])


def subscribe(pvname, mode):
    """ Requests subscription to ``pvname`` with mode ``mode``.
        
        Returns a unique ID for the request.

        :param mode: Subclass of :class:`SubscriptionMode`.

    """
    pv = get_info(pvname)
    if not pv:
        datastore.save_pv(pvname, mode)

        task = Task(pvname, epics_subscribe, pvname, mode)
        receipt = workers.request(task)
        if mode.name == SubscriptionMode.Scan.name:
            # in addition, add it to the timer so that it gets scanned
            # periodically
            periodic_task = Task(pvname, epics_periodic, pvname, mode.period)
            timers.schedule(periodic_task, mode.period)

        return receipt
    else:
        msg = "Already subscribed to PV '%s'" % pv.name
        logger.warn(msg)
        raise ValueError(msg)

def unsubscribe(pvname_pattern):
    """ Stops archiving the PVs that match the given pattern. 
    
        Returns a the request ids for all the generated tasks.
    """
    known_apvs = get_pvs(pvname_pattern)
    reqids = []
    for apv in known_apvs:
        datastore.remove_pv(apv.name)
        task = Task(apv.name , epics_unsubscribe, apv.name)

        if isinstance(apv.mode, SubscriptionMode.Scan):
            # cancel timers 
            timers.remove_task(task.name)

        reqids.append(workers.request(task))
    return reqids

def get_result(reqid):
    return workers.get_result(reqid)

def get_info(pvname):
    """ Returns an :class:`ArchivedPV` representing the PV. 

        If there's no subscription to that PV, ``None`` is returned.
    """
    return datastore.read_pv(pvname) 

def get_status(pvname):
    """ Returns the status dictionary for the given PV.  """
    return datastore.read_status(pvname)

def get_values(pvname, fields=[], limit=100, from_date=None, to_date=None):
    """ Returns latest archived data for the PV as a list with at most ``limit`` elements """
    return datastore.read_values(pvname, fields, limit, from_date, to_date)

def get_pvs(pvname_pattern='*', modename_pattern='*'):
    """ Returns an iterator over the PVs known by the archiver in :class:`ArchivedPV` form """
    return datastore.list_pvs(pvname_pattern, modename_pattern)

def load_config(fileobj):
    """ Restore the state defined by the config.
    
        Returns a list with the receipts of the restored subscriptions.
    """
    receipts = []
    for line in fileobj:
        d_line = json.loads(line)
        mode_dict = d_line['mode']
        name = d_line['name']
        mode = SubscriptionMode.parse(mode_dict)
        receipt = subscribe(name, mode)
        receipts.append(receipt)
    return receipts

def save_config(fileobj):
    """ Save current subscription state. """
    # get a list of the ArchivedPV values 
    apvs = get_pvs()
    for apv in apvs:
        del apv['since']
        fileobj.write(json.dumps(apv) + '\n', )

def shutdown():
    logger.info("Unsubscribing to all subscribed PVs...")
    unsubscribe('*')

    if workers.running:
        workers.stop()
    if timers.running:
        timers.stop()


    logger.info("Shutdown completed")

def initialize(replication_factor=2, recreate=False):
    datastore.create_schema(config.DATASTORE['servers'][0], config.DATASTORE['keyspace'], replication_factor, recreate)


##################### TASKS #######################
def epics_subscribe(state, pvname, mode):
    """ Function to be run by the worker in order to subscribe """
    import epics
    logger.info("Subscribing to '%s' with mode '%s'", pvname, mode)
    conn_timeout = config.CONTROLLER['epics_connection_timeout']

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
    logger.info("Unsubscribing to %s", pvname)
    pv = state['pv']
    pv.disconnect()
    del state['pv']

    datastore.remove_pv(pvname)
    return True # needed to signal that the subscription req has been completed.
                # for example, to be able to sync on controller.get_result 


def epics_periodic(state, pvname, period):
    """ Invoked every time a period for a scanned PV is due """
    logger.debug("Periodic scan for PV '%s' with period %f secs", pvname, period)
    
    # get the pv's value
    pv = state.get('pv')
    if pv:
        if pv.status == 1: # connected
            #generate the id for the update
            update_id = uuid.uuid1()
            data = _gather_pv_data(pv)
            datastore.save_update(update_id, **data) 
        else: # not connected
            logger.warn("Ignoring non-connected PV '%s'", pvname)
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

def _gather_pv_data(pv):
    to_consider = ('pvname', 'value', 'count', 'type', 'status', 'precision', 'units', 'severity', \
                    'timestamp', 'access', 'host', 'upper_disp_limit', 'lower_disp_limit', \
                    'upper_alarm_limit', 'lower_alarm_limit','upper_warning_limit', 'lower_warning_limit', \
                    'upper_ctrl_limit', 'lower_ctrl_limit')
    
    data = dict( (k, getattr(pv, k)) for k in to_consider)
    dt = datetime.datetime.fromtimestamp(data['timestamp'])
    data['datetime'] = dt.isoformat(' ')
    return data

##############################################################

