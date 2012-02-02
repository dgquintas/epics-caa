import multiprocessing
from multiprocessing import cpu_count, Process, current_process
import threading
import uuid
from zlib import adler32
try: 
    import queue
except:
    import Queue as queue

import time
import collections
import logging

import caa.config as config

logger = logging.getLogger('tasks')

class Task(object):
    """ Encapsulates a function alongside its arguments.
    
        In order to run it, simply call it without any arguments::

            t = Task('the name', f, (arg1, arg2...))
            t() # run it
        
    """
    def __init__(self, name, f, *args):
        """ :param name: A textual id for the task
            :param f: The function to invoke when calling the task
            :param args: tuple of arguments to be passed to `f`
        """

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
    """ A thread that takes care of invoking periodic events """

    STOP_SENTINEL = '__STOP'

    def __init__(self, inq, name, workers):
        """ :param inq: synchronized LIFO queue where :class:`Task` instances are
                        submitted in order to add them to the timer.
            :param name: a textual name for the timer thread.
        """
        threading.Thread.__init__(self, name=name)
        self._inq = inq
        self.tasks = []
        self.state = collections.defaultdict(dict)
        self.workers = workers

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
                    # run the task to be invoked when period is over
                    self.workers.request(task)

                    # update enqueue time 
                    self.tasks[i][0] = t

        logger.info("Timer %s exiting.", threading.current_thread().name)

##############################################


class TimersPool(object):
    """ Pool of :class:`TimerThread`s """

    def __init__(self, workers, num_timers=2):
        self._task_queues = []
        self._timers = []
        for i in range(num_timers):
            tq = queue.Queue()
            timer = TimerThread(tq, 'Timer-%d' % i, workers)
            
            self._task_queues.append(tq)
            self._timers.append(timer)

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
        chosen_q = self._task_queues[h]
        chosen_q.put((reqid, task, period))

        return reqid

    def subscribe(self, task):
        logger.debug("Submitting subscription request to '%s'", task.name)
        # submit subscription request to queue
        return self._submit(task)

    def request(self, task, period):
        """ Add `task` to one of the pool's timers. 
        
            The given `task` will be run every `period` seconds.

            :param task: the task to add to one of the pool's timers.
            :param period: a float representing the period for `task`, in seconds (or fractions thereof).
        """
        logger.debug("Submitting periodic request '%s' with period %f", task, period)
        # submit subscription request to queue
        return self._submit(task, period)

    def start(self):
        """ Start all the pool's :class:`TimerThread`s """
        if self._running: 
            logger.warn("Timers already running.")
        else:
            for t in self._timers:
                t.daemon = True
                logger.info("Starting timer %s", t.name)
                t.start()

    def stop(self):
        """ Stop all the pool's :class:`TimerThread`s """
        if not self._running:
            logger.warn("Timers weren't running.")
        else:
            logger.info("Stopping timers...")
            [ q.put(TimerThread.STOP_SENTINEL) for q in self._task_queues ]
            self.join()
            self._running = False

    def join(self):
        """ Blocks until all pending requests have finished """
        logger.debug("%s blocking until all pending requests finish.", self)
        [ t.join() for t in self._timers ]


#################################

class Worker(Process):

    STOP_SENTINEL = '__STOP'

    def __init__(self, inq, outq, name):
        Process.__init__(self, name=name)
        self._inq = inq
        self._outq = outq

        self.state = collections.defaultdict(dict)

    def run(self):
        logger.debug("At worker's run() method")
        for reqid, task in iter(self._inq.get, self.STOP_SENTINEL):
            logger.debug("Processing task '%s' with id '%s'", task, reqid)
            
            t_res = task(self.state[task.name])  

            task.result = t_res
            logger.debug("Task with id '%s' for PV '%s' completed. Result: '%r'", \
                    reqid, task.name, task.result )

            self._outq.put((reqid, task))

            if not self.state[task.name]:
                del self.state[task.name]

        logger.info("Worker exiting.")


class WorkersPool(object):
    """ Pool of :class:`Worker`s.
    
        They manage PV's callbacks as well as ``get`` requests from the 
        scanned PVs.
    """


    def __init__(self, num_workers=cpu_count()):
        self._done_queue = multiprocessing.Queue()
        self._task_queues = []
        self._workers = []
        for i in range(num_workers):
            tq = multiprocessing.Queue()
            w = Worker(tq, self._done_queue, "Worker-%d" % i)

            self._task_queues.append(tq)
            self._workers.append(w)

        self._req_lock = threading.Lock()
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
        chosen_q = self._task_queues[h]
        chosen_q.put((reqid, task))

        return reqid

    def request(self, task):
        """ Submit a :class:`Task` instance to be run by one of the processes """
        self._req_lock.acquire()
        logger.debug("Acquired request lock for task '%s'", task)
        logger.debug("Submitting request '%s'", task)
        # submit subscription request to queue
        res = self._submit(task)
        self._req_lock.release()
        logger.debug("Released request lock for task '%s'", task)
        return res


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
        logger.debug("%s blocking until all pending requests finish.", self)
        [ w.join() for w in self._workers ]


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
            [ q.put(Worker.STOP_SENTINEL) for q in self._task_queues ]
            self.join()
            self._running = False


##############################################



