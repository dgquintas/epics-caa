from __future__ import print_function

import json
import logging
import uuid
import time
import collections
from multiprocessing import cpu_count, Process, current_process, JoinableQueue, Queue
from zlib import adler32

from caa import ArchivedPV, SubscriptionMode
import datastore
import caa.config as config

import epics

"""
.. module:: controller
    :synopsis: Interface to the outside world

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>
"""

logger = logging.getLogger('controller')


#######################################################

class Task(object):
    def __init__(self, name, f, *args):
        self.name = name
        self.f = f
        self.args = args
        self.done = False

    def __call__(self, state):
        return self.f(state, *self.args)

    def __str__(self):
        return 'Task<%s(%r)>. Done: %s' % (self.name, self.args, self.done)


class WorkersPool(object):

    STOP_SENTINEL = '__STOP'

    def __init__(self, num_workers=cpu_count()):
        self._done_queue = Queue()
        self._workers = [ Process(target=self.worker, name=("Worker-%d"%i)) for i in range(num_workers) ]
        self._task_queues = dict( (w.name, JoinableQueue()) for w in self._workers  )
        self._running = False
        

    @property
    def num_workers(self):
        return len(self._workers)

    def _submit(self, task):  
        if not self._running:
            self.start()
            self._running = True

        reqid = uuid.uuid4()
        # calculate the hash for the task based on its name
        h = adler32(task.name) % self.num_workers
        chosen_worker = self._workers[h]
        self._task_queues[chosen_worker.name].put((reqid, task))

        return reqid

        
    def request_subscription(self, sub_task):
        """ Adds a :class:`Task` to the pool. 

            Returns a unique id that can be used to identify the subscription request 
            upon completion.

            :param Task: a :class:`Task` instance 

        """
        logger.info("Submitting subscription request '%s'", sub_task)
        return self._submit(sub_task)

    def request_unsubscription(self, unsub_task):
        logger.info("Submitting unsubscription request '%s'", unsub_task)
        return self._submit(unsub_task)


    def worker(self):
        wname = current_process().name
        logger.debug("At worker function '%s'", wname)
        state = collections.defaultdict(dict)
        q = self._task_queues[wname]
        for reqid, task in iter(q.get, self.STOP_SENTINEL):
            logger.debug("Worker '%s' processing task '%s' with id '%s'", 
                    wname, task, reqid)
            
            t_res = task(state[task.name])  

            task.result = t_res
            logger.debug("Task with id '%s' for PV '%s' completed. Result: '%r'", \
                    reqid, task.name, task.result )
            self._done_queue.put((reqid, task))
            q.task_done()

            if not state[task.name]:
                del state[task.name]
        logger.info("Worker %s exiting: ", current_process())



    def get_result(self, block=True, timeout=None):
        """ Returns the receipt and *a* :class:``Task`` instance with 
            a populated ``result`` attribute. 

            :param boolean block: If ``True``, will block until a result in available. Otherwise,
            return an item if one is immediately available, else raise :exc:`Queue.Empty`.
            :param float timeout: If blocking, how many seconds to wait. If no result 
            is available after that many seconds, :exc:`Queue.Empty` exception is raised.
            :rtype: a list of the form ``(receipt, :class:`Task`-instance)``
        """
        return self._done_queue.get(block, timeout)
 
    def join(self):
        """ Blocks until all pending requests have finished """
        logger.debug("Blocking until all pending requests finish.")
        [ q.join for q in self._task_queues.itervalues() ]


    def start(self):
        if self._running: 
            logger.warn("Workers already running.")
        else:
            for w in self._workers:
                w.daemon = True
                logger.info("Starting worker %s", w)
                w.start()

    def stop(self):
        if not self._running:
            logger.warn("Workers weren't running.")
        else:
            logger.info("Stopping workers...")
            [ q.put(self.STOP_SENTINEL) for q in self._task_queues.itervalues() ]
            self._running = False


##############################################

workers = WorkersPool(config.CONTROLLER['num_workers'])

def subscribe(pvname, mode=SubscriptionMode.MONITOR, scan_period=0, monitor_delta=0):
    """ Adds the PV to the list of PVs to be archived. 
    
        Returns a unique id (receipt) for the subscription request. 
    """
    apv = ArchivedPV(pvname, mode, scan_period, monitor_delta)
    datastore.save_pv(apv)

    # submit subscription request to queue
    request = Task(pvname , epics_subscribe, apv)
    return workers.request_subscription(request)


def unsubscribe(pvname):
    """ Stops archiving the PV """
    apv = get_info(pvname)
    #FIXME: complain if missing key?

    if apv:
        datastore.remove_pv(apv)
        request = Task(apv.name , epics_unsubscribe, apv)
        return workers.request_unsubscription(request)
    return False

def get_result(block=True, timeout=None):
    """ Returns the receipt and *a* :class:`Task` instance with a ``result`` attribute.

        See :meth:`WorkersPool.get_result` 
    """
    return workers.get_result()


def get_info(pvname):
    """ Returns an :class:`ArchivedPV` instance for the given subscribed PV name.

        If there's no subscription to that PV, ``None`` is returned.
    """
    return datastore.read_pv(pvname) 

def get_status(pvnames):
    """ Returns a dictionary keyed by the PV name containing their
        status information. 

        The status information, if present, is represented by a dictionary keyed by
        ``timestamp`` and ``connected``. If no information is present for
        a given PV, an empty dictionary is returned.
    """
    return datastore.read_latest_status(pvnames)


def get_values(pvname, limit=100):
    """ Returns latest archived data for the PV as a list with at most ``limit`` elements """
    return datastore.read_latest_values(pvname, limit)


def load_config(fileobj):
    """ Restore the state defined by the config.
    
        Returns a list with the receipts of the restored subscriptions.
    """
    receipts = []
    for line in fileobj:
        d = dict(json.loads(line))
        receipt = subscribe(d['name'], d['mode'], d['scan_period'], d['monitor_delta'])
        receipts.append(receipt)
    return receipt


def save_config(fileobj):
    """ Save current subscription state. """
    # get a list of the ArchivedPV values 
    for archived_pv in subscriptions.values():
        fileobj.write(json.dumps(archived_pv, cls=ArchivedPV.APVJSONEncoder) + '\n')

def shutdown():
    workers.stop()


##################### TASKS #######################
def epics_subscribe(state, archived_pv):
    """ Function to be run by the worker in order to subscribe """
    logger.info("EPICS-subscribing to %s", archived_pv)
    datastore.save_pv(archived_pv)
    conn_timeout = config.CONTROLLER['epics_connection_timeout']

    # DBE_VALUE--when the channel's value changes by more than MDEL.
    # DBE_LOG--when the channel's value changes by more than ADEL.
    # DBE_ALARM--when the channel's alarm state changes.
    sub_mask = epics.dbr.DBE_ALARM | epics.dbr.DBE_LOG | epics.dbr.DBE_VALUE

    pv = epics.PV(archived_pv.name, 
            callback=subscription_cb, 
            connection_callback=connection_cb,             
            connection_timeout=conn_timeout,
            auto_monitor=sub_mask
            )
    connected = pv.connected
    connection_cb(archived_pv.name, connected) # we need it for pv's that can't connect at startup

    state['pv'] = pv
    return True

def epics_unsubscribe(state, archived_pv):
    """ Function to be run by the worker in order to unsubscribe """
    logger.info("EPICS-unsubscribing to %s", archived_pv)
    pv = state['pv']
    pv.disconnect()
    del state['pv']

    datastore.remove_pv(archived_pv)
    return True



##################### CALLBACKS #######################
def subscription_cb(**kw):
    """ Internally called when a new value arrives for a subscribed PV. """
    logger.debug("PV callback invoked: %(pvname)s changed to value %(value)s at %(timestamp)s" % kw)

    #generate the id for the update
    update_id = uuid.uuid1()
    datastore.save_update(update_id, kw['pvname'], kw['value'], kw['type']) #TODO: add more stuff

def connection_cb(pvname, conn, **kw):
    logger.debug("PV '%s' connected: %s", pvname, conn)
    
    status_id = uuid.uuid1()
    datastore.save_status(status_id, pvname, conn)



