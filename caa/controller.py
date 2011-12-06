from __future__ import print_function

import json
import logging
import uuid
import time 
import collections
from multiprocessing import cpu_count, Process, current_process, JoinableQueue, Queue
import threading
from zlib import adler32
try: 
    import queue
except:
    import Queue as queue

from caa import ArchivedPV, SubscriptionMode
import datastore
import caa.config as config

import epics

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

    def __repr__(self):
        return 'Task<%s(%r)>. Done: %s' % (self.name, self.args, self.done)


#######################################################

class TimerThread(threading.Thread):

    STOP_SENTINEL = '__STOP'

    def __init__(self, inq, name):
        threading.Thread.__init__(self, name=name)
        self._inq = inq
        self.tasks = []

    def run(self):
        while True:
            t = time.time()
            try:
                item = self._inq.get(block=True, timeout=0.1)
                logger.debug("Gotten item %s", item)
                if item == self.STOP_SENTINEL:
                    break
                else:
                    reqid, task, period = item
                    logger.debug("Adding task '%s' with id '%s' and period '%f'", 
                            task, reqid, period)
                    self.tasks.append([t,task, period])

            except queue.Empty: 
                pass

            # traverse task list checking for ripe ones
            for i, (enq_t, task, period) in enumerate(self.tasks):
                if t - enq_t >= period:
                    # submit it to the wokers
                    workers.request(task)
                    # update enqueue time 
                    self.tasks[i][0] = t

        logger.info("Timer %s exiting: ", threading.current_thread().name)

##############################################


class TimersPool(object):

    def __init__(self, num_timers=2):
        names = ['Timer-%d' % i for i in range(num_timers)]
        self._inq = dict( (name, queue.Queue()) for name in names )

        self._timers = []
        for name in names:
            q = self._inq[name]
            self._timers.append( TimerThread(q, name) )

        self._running = False

    @property
    def running(self):
        return self._running

    @property
    def num_timers(self):
        return len(self._timers)

    def _submit(self, task, period):
        if not self._running:
            self.start()
            self._running = True

        reqid = uuid.uuid4()
        # calculate the hash for the task based on its name
        h = adler32(task.name) % self.num_timers
        chosen_timer = self._timers[h]
        logger.info("Enqueuing task '%s' in '%s'\'s queue", task.name, chosen_timer.name)
        self._inq[chosen_timer.name].put((reqid, task, period))

        return reqid

    def request(self, task, period):
        logger.debug("Submitting periodic request '%s' with period %f", task, period)
        # submit subscription request to queue
        return self._submit(task, period)

    def start(self):
        if self._running: 
            logger.warn("Timers already running.")
        else:
            for t in self._timers:
                t.daemon = True
                logger.info("Starting timer %s", t.name)
                t.start()

    def stop(self):
        if not self._running:
            logger.warn("Timers weren't running.")
        else:
            logger.info("Stopping timers...")
            [ q.put(TimerThread.STOP_SENTINEL) for q in self._inq.itervalues() ]
            self._running = False

#################################

class WorkersPool(object):

    STOP_SENTINEL = '__STOP'

    def __init__(self, num_workers=cpu_count()):
        self._done_queue = Queue()
        self._workers = [ Process(target=self.worker, name=("Worker-%d"%i)) for i in range(num_workers) ]
        self._task_queues = dict( (w.name, JoinableQueue()) for w in self._workers  )
        self._running = False
        
    @property
    def running(self):
        return self._running

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

    def request(self, task):
        logger.debug("Submitting request '%s'", task)
        # submit subscription request to queue
        return self._submit(task)

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
        logger.info("Worker %s exiting: ", current_process().name)


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
                logger.info("Starting worker %s", w.name)
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
timers = TimersPool(config.CONTROLLER['num_timers'])

def subscribe(pvname, mode):
    """ Requests subscription to ``pvname`` with mode ``mode``.
        
        Returns a unique ID for the request.

        :param mode: Subclass of :class:`SubscriptionMode`.

    """
    datastore.save_pv(pvname, mode)

    task = Task(pvname, epics_subscribe, pvname, mode)
    if mode.name == SubscriptionMode.Scan.name:
        # in addition, add it to the timer so that it gets scanned
        # periodically
        periodic_task = Task(pvname, epics_periodic, pvname, mode.period)
        timers.request(periodic_task, mode.period)

    return workers.request(task)

def unsubscribe(pvname):
    """ Stops archiving the PV. 
    
        Returns a unique request id or `False` if the given `pvname` was unknown.
    """
    apv = get_info(pvname)
    if apv:
        datastore.remove_pv(apv.name)
        task = Task(apv.name , epics_unsubscribe, apv.name)
        return workers.request(task)
    else:
        logger.warn("Requesting unsubscription to unknown PV '%s'", pvname)
        return False

def get_result(block=True, timeout=None):
    """ Returns the receipt and *a* :class:`Task` instance with a ``result`` attribute.

        See :meth:`WorkersPool.get_result` 
    """
    return workers.get_result()


def get_info(pvname):
    """ Returns an :class:`ArchivedPV` representing the PV. 

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
        name, mode_dict, _ = json.loads(line)
        mode = SubscriptionMode.parse(mode_dict)
        receipt = subscribe(name, mode)
        receipts.append(receipt)
    return receipt

def save_config(fileobj):
    """ Save current subscription state. """
    # get a list of the ArchivedPV values 
    apvs = datastore.list_pvs()
    for apv in apvs:
        fileobj.write(json.dumps(apv, cls=ArchivedPV.JSONEncoder) + '\n', )

def shutdown():
    if workers.running:
        workers.stop()
    if timers.running:
        timers.stop()


##################### TASKS #######################
def epics_subscribe(state, pvname, mode):
    """ Function to be run by the worker in order to subscribe """
    logger.info("%s: EPICS-subscribing to %s", current_process(), pvname)
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
    connected = pv.connected
    connection_cb(pvname, connected) # we need it for pv's that can't connect at startup

    state['pv'] = pv
    return True

def epics_unsubscribe(state, pvname):
    """ Function to be run by the worker in order to unsubscribe """
    logger.info("%s: EPICS-unsubscribing to %s", current_process(), pvname)
    pv = state['pv']
    pv.disconnect()
    del state['pv']

    datastore.remove_pv(pvname)
    return True

def epics_periodic(state, pvname, period):
    """ Invoked every time a period for a scanned PV is due """
    logger.debug("Periodic scan for PV '%s' with period %f secs", pvname, period)
    
    # get the pv's value
    pv = state['pv']
    value = pv.get()
    _type = pv.type

    #generate the id for the update
    update_id = uuid.uuid1()
    datastore.save_update(update_id, pvname, value, _type) #TODO: add more stuff


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



