import multiprocessing
from multiprocessing import cpu_count, Process
from multiprocessing.queues import SimpleQueue
import threading
import itertools
from zlib import adler32
try: 
    import queue
except:
    import Queue as queue

import time
import collections
import logging

logger = logging.getLogger('tasks')


task_id_generator = itertools.count()

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

    def __call__(self, state):
        return self.f(state, *self.args)

    def __repr__(self):
        return 'Task-%s<%s(%r)>' % (self.name, self.f.__name__, self.args)

# TaskResult = collections.namedtuple('TaskResult', 'name, result, timestamp')

def result_handler(fromworkers_q, pending_futures):
        logger.info("Starting result handler thread")
        get = fromworkers_q.get

        while True:
            task = get()
            if not task: # got sentinel
                break

            futureid, result = task
            pending_futures[futureid]._set(result)

        while pending_futures: # flush remaining
            task = get()
            if not task: # ignore extra sentinel
               continue 

            futureid, result = task
            pending_futures[futureid]._set(result)

        logger.info("Result handler thread exiting...")


# based on multiprocessing.pool.ApplyResult
class TaskFuture(object):
    def __init__(self, taskname, callback, pending_futures):
        self._taskname = taskname
        self._cond = threading.Condition( threading.Lock() )
        self._ready = False
        self._callback = callback
        self._taskid = task_id_generator.next()
        self._pending_futures = pending_futures
        self._pending_futures[self._taskid] = self

    @property
    def taskname(self):
        return self._taskname

    @property
    def futureid(self):
        return self._taskid 

    @property
    def ready(self):
        return self._ready

    @property 
    def successful(self):
        assert self._ready
        return self._success

    def __repr__(self):
        return "TaskResult(futureid=%d, ready=%s, callback=%s)" % \
                (self.futureid, self.ready, self._callback)

    def wait(self, timeout=None):
        self._cond.acquire()
        try:
            if not self._ready:
                self._cond.wait(timeout)
        finally:
            self._cond.release()

    def get(self, timeout=None):
        self.wait(timeout)
        if not self._ready:
            raise multiprocessing.TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    # invoked from ResultsHandlerThread
    def _set(self, result):
        self._success, self._value = result
        if self._callback and self._success:
            self._callback(self._value)
        self._cond.acquire()
        try:
            self._ready = True
            self._cond.notify()
        finally:
            self._cond.release()
        del self._pending_futures[self._taskid] 


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
                    task, period, callback = args
                    logger.debug("Adding task '%s' with period '%s'", 
                            task, period)
                    self.tasks[task.name] = [t, task, period, callback] 
                else:
                    logger.error("Unknown action '%s'", action)

            except queue.Empty: 
                pass

            # traverse task list checking for ripe ones
            for enq_t, task, period, callback in self.tasks.itervalues():
                if t - enq_t >= period:
                    # run the task to be invoked when period is over
                    logger.debug("Time up for task '%s'", task)
                    self.workers.request(task, callback) 
                    # update enqueue time 
                    self.tasks[task.name][0] = t #FIXME: fugly

        logger.info("Timer %s exiting.", threading.current_thread().name)

##############################################


class TimersPool(object):
    """ Pool of :class:`TimerThread`s """

    def __init__(self, workers, num_timers=2):
        self._task_queues = []
        self._timers = []
        self._workers = workers
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

    def schedule(self, task, period, callback=None):
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

        logger.debug("Submitting periodic request '%s' with period '%s's", task, period)
        q.put((TimerThread.ADD_TASK_TOKEN, (task, period, callback) ))

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

    def __init__(self, inq, outq, name):
        Process.__init__(self, name=name)
        self._inq = inq
        self._outq = outq

        self.state = collections.defaultdict(dict)

    def run(self):
        logger.debug("At worker's run() method")
        while True:
            taskbundle = self._inq.get()

            if not taskbundle: # got sentinel
                break
            
            taskid, task = taskbundle
            logger.debug("Processing task '%s' with id '%s'.", task, taskid)

            try:
                result = (True, task(self.state[task.name]))  
                logger.debug("Task with id '%s' for PV '%s' completed. Result: '%r'", \
                        taskid, task.name, result[1])
            except Exception, e:
                result = (False, e)

            # post the result to a queue, from whence
            # it'll be read by (most likely) a thread that'll
            # take care of populating the corresponding
            # "future" with it
            self._outq.put((taskid, result))

            if not self.state[task.name]:
                del self.state[task.name]

        logger.info("Worker exiting.")


class WorkersPool(object):
    """ Pool of :class:`Worker`s.
    
        They manage PV's callbacks as well as ``get`` requests from the 
        scanned PVs.
    """
    def __init__(self, num_workers=cpu_count()):
        self._fromworkers_q = SimpleQueue()
        self._toworkers_qs = []
        self._workers = []
        self._pending_futures = {}
        for i in range(num_workers):
            toworker_q = SimpleQueue()
            w = Worker(toworker_q, self._fromworkers_q, "Worker-%d" % i)

            self._toworkers_qs.append(toworker_q)
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

    def _submit(self, task, callback):
        # XXX: instead of a reqid, return a 
        # "Future", which holds a reference to 
        # the place where the result will be stored
        # by the worker (some Managed instance, shared
        # by the Future and the worker process)
        # candidate: 
        taskfuture = TaskFuture(task.name, callback, self._pending_futures)
        # calculate the hash for the task based on its name
        h = adler32(task.name) % self.num_workers
        toworker_q = self._toworkers_qs[h]
        toworker_q.put((taskfuture.futureid, task)) 

        if not self._workers[h].is_alive():
            msg = "Worker '%d' has died!" % h
            logger.critical(msg)
            raise RuntimeError(msg)

        return taskfuture 

    def request(self, task, callback=None):
        """ Submit a :class:`Task` instance to be run by one of the processes """
        with self._req_lock:
            logger.debug("Acquired request lock for task '%s'", task)
            if not self._running:
                self.start()

            # submit subscription request to queue
            if not self._stopping:
                # don't accept requests if we are shutting down
                logger.debug("Submitting request '%s'", task)
                taskfuture = self._submit(task, callback)
            else: 
                logger.warn("Request for '%s' received while shutting down", task)
                taskfuture = None

        logger.debug("Released request lock for task '%s'", task)
        return taskfuture 


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

            self._result_handler_thread = threading.Thread(
                    target = result_handler, 
                    name = "ResultsHandlerThread",
                    args = (self._fromworkers_q, self._pending_futures))
            self._result_handler_thread.daemon = True
            self._result_handler_thread.start()


    def stop(self):
        if not self._running:
            logger.warn("Workers weren't running.")
        else:
            logger.info("Stopping workers...")
            self._stopping = True
            [ q.put(None) for q in self._toworkers_qs]
            self.join()
            self._running = False
            self._stopping = False


##############################################



