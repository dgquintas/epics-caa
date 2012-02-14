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
        return 'Task-%s<%s(%r)>. Done: %s' % (self.name, self.f.__name__, self.args, self.done)


#######################################################

class TimerThread(threading.Thread):
    """ A thread that takes care of invoking periodic events """

    STOP_SENTINEL = '__STOP'
    REMOVE_TASK_TOKEN = '__REMOVE_TASK'
    ADD_TASK_TOKEN = '__ADD_TASK'

    def __init__(self, inq, name, workers):
        """ :param inq: synchronized LIFO queue where :class:`Task` instances are
                        submitted in order to add them to the timer.
            :param name: a textual name for the timer thread.
        """
        threading.Thread.__init__(self, name=name)
        self._inq = inq
        self.tasks = {}
        self.workers = workers

    def run(self):
        while True:
            t = time.time()
            try:
                action, args = self._inq.get(block=True, timeout=0.1)
                logger.debug("Gotten action '%s' with args '%s'", action, args)
                if action == self.STOP_SENTINEL:
                    break
                elif action == self.REMOVE_TASK_TOKEN:
                    try:
                        task_name = args[0]
                        removed = self.tasks.pop(task_name)
                        logger.debug("Removed '%s' from timer thread", str(removed))
                    except KeyError:
                        logger.error("Attempted to remove unknown task '%s' from timer thread", task_name)

                elif action == self.ADD_TASK_TOKEN:
                    task, period = args
                    logger.debug("Adding task '%s' with period '%f'", 
                            task, period)
                    self.tasks[task.name] = [t,task, period]
                else:
                    logger.error("Unknown action '%s'", action)

            except queue.Empty: 
                pass

            # traverse task list checking for ripe ones
            for enq_t, task, period in self.tasks.itervalues():
                if t - enq_t >= period:
                    # run the task to be invoked when period is over
                    logger.debug("Time up for task '%s'", task)
                    self.workers.request(task)

                    # update enqueue time 
                    self.tasks[task.name][0] = t

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

    @property 
    def num_active_tasks(self):
        return sum( len(t.tasks) for t in self._timers )

    def _get_queue_for(self, name):
        h = adler32(name) % self.num_timers
        chosen_q = self._task_queues[h]
        return chosen_q

    def request(self, task, period):
        """ Add `task` to one of the pool's timers. 
        
            The given `task` will be run every `period` seconds.

            :param task: the task to add to one of the pool's timers.
            :param period: a float representing the period for `task`, in seconds (or fractions thereof).
        """
        if not self._running:
            self.start()

        if period <= 0:
            raise ValueError("Invalid period (%s). Must be > 0", period)

        q = self._get_queue_for(task.name)
        logger.debug("Submitting periodic request '%s' with period %f s", task, period)
        q.put((TimerThread.ADD_TASK_TOKEN, (task, period) ))

    def remove_task(self, taskname):
        q = self._get_queue_for(taskname)
        logger.debug("Removing periodic task '%s'", taskname)
        q.put( (TimerThread.REMOVE_TASK_TOKEN, (taskname,) ) )

    def start(self):
        """ Start all the pool's :class:`TimerThread`s """
        if self._running: 
            logger.warn("Timers already running.")
        else:
            self._running = True
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
            [ q.put((TimerThread.STOP_SENTINEL, None)) for q in self._task_queues ]
            self.join()
            self._running = False

    def join(self):
        """ Blocks until all pending requests have finished """
        logger.debug("%s blocking until all pending requests finish.", self)
        [ t.join() for t in self._timers ]
        logger.debug("%s joined all its threads", self)


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
            logger.debug("Processing task '%s' with id '%s'.Queue size: %d ", task, reqid, self._inq.qsize())
            
            t_res = task(self.state[task.name])  
            

            task.result = t_res
            task.done = True
            logger.debug("Task with id '%s' for PV '%s' completed. Result: '%r'", \
                    reqid, task.name, task.result )

            if t_res:
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
        self._stopping = False
        
    @property
    def running(self):
        return self._running

    @property
    def num_workers(self):
        return len(self._workers)

    def _submit(self, task):  
        reqid = uuid.uuid4()
        # calculate the hash for the task based on its name
        h = adler32(task.name) % self.num_workers
        chosen_q = self._task_queues[h]
        chosen_q.put((reqid, task))
        if not self._workers[h].is_alive():
            msg = "Worker '%d' has died!" % h
            logger.critical(msg)
            raise RuntimeError(msg)

        return reqid

    def request(self, task):
        """ Submit a :class:`Task` instance to be run by one of the processes """
        self._req_lock.acquire()
        logger.debug("Acquired request lock for task '%s'", task)
        if not self._running:
            self.start()

        # submit subscription request to queue
        if not self._stopping:
            # don't accept requests if we are shutting down
            logger.debug("Submitting request '%s'", task)
            res = self._submit(task)
        else: 
            logger.warn("Request for '%s' received while shutting down", task)
            res = None

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
            self._running = True
            for w in self._workers:
                w.daemon = True
                logger.info("Starting worker %s", w.name)
                w.start()

    def stop(self):
        if not self._running:
            logger.warn("Workers weren't running.")
        else:
            logger.info("Stopping workers...")
            self._stopping = True
            [ q.put(Worker.STOP_SENTINEL) for q in self._task_queues ]
            self.join()
            self._running = False
            self._stopping = False


##############################################



