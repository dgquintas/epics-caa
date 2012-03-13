import unittest
import logging
import time
import random
import multiprocessing
import pdb
import functools
from collections import defaultdict

from caa.tasks import Task, WorkersPool, TimersPool

logging.basicConfig(level=logging.INFO, format='[%(processName)s/%(threadName)s] %(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('TestTasks')


def wait_for_reqids(getterf, reqids):
    res = []
    for reqid in reqids:
        while True:
            r = getterf(reqid)
            if r:
                res.append(r)
                break
            time.sleep(0.1)
    return res

###################################

def f(state, a,b,c):
    time.sleep(2)
    return a+b+c

def g(state):
    return time.time()

class TestTasks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        self.workers = WorkersPool()
        self.workers.start()

        self.timers = TimersPool(self.workers)
        self.timers.start()

    def test_workerspool_wait(self):
        tasks = [ Task('task%d'%i, f, i+1,i+2,i+3) for i in range( self.workers.num_workers ) ]
        futures = [ self.workers.request(task) for task in tasks ]
        
        print "waiting..."
        [ fut.wait() for fut in futures ]
        print "done!"

        for i, fut in enumerate(futures):
            self.assertTrue( fut.ready )
            self.assertTrue( fut.successful )
            result = fut.get()
            self.assertEqual( i+1+i+2+i+3, result )
 
    def test_workerspool_blocking_get(self):
        tasks = [ Task('task%d'%i, f, i+1,i+2,i+3) for i in range( self.workers.num_workers ) ]
        futures = [ self.workers.request(task) for task in tasks ]
        
        print "waiting..."
        for i, fut in enumerate(futures):
            result = fut.get() # this ought to block
            self.assertTrue( result )
            self.assertEqual( i+1+i+2+i+3, result )
        print "done!"

    def test_workerspool_timeout_get(self):
        task = Task('task', f, 1,2,3) 
        future = self.workers.request(task) 
        
        print "waiting..."
        self.assertRaises(multiprocessing.TimeoutError, future.get, timeout=1)
        print "done!"

    def test_workerspool_callback(self):
        class NS: pass
        ns = NS() #hacky way to the able to modify enclosure vars
        ns.cb_invoked = False
        def cb(value):
            ns.cb_invoked = True
            self.assertEqual(value, 1+2+3)

        task = Task('task', f, 1,2,3) 
        future = self.workers.request(task, cb) 
        
        print "waiting..."
        future.wait()
        print "done!"
        self.assertTrue(ns.cb_invoked)


#############################

    def test_timer_callbacks(self):
        results = defaultdict(list)
        def wrapper(src):
            def cb(value):
                logger.info("Result %f from '%s'", value, src)
                results[src].append(value)
            return cb

        n_timers = 4

        tasks = [ Task('task%d'%i, g) for i in range(n_timers) ]

        cbs = [ wrapper(task.name) for task in tasks ]
        for i,task in enumerate(tasks):
            self.timers.schedule(task, period=i+1, callback=cbs[i]) 

        # periods go from 1 to n_timers

        sleep_for = 12  # lcm of 1,2,3,4
        time.sleep(sleep_for+0.2) 
        self.timers.stop() # implicit join

        for i,task in enumerate(tasks):
            result_for_task = results[task.name]
            self.assertEqual( len(result_for_task), sleep_for//(i+1) )

    def test_timer_values(self):
        results = []
        def cb(value):
            results.append(value)

        task = Task('task', g)
        period = random.randint(1,5)
        reps = random.randint(1,5)
        self.timers.schedule(task, period, cb)

        time.sleep(reps*period + 0.2) 
        self.timers.stop() # implicit join
        
        self.assertEqual(len(results), reps)
        task0res = results[0]
        for i in range(1,len(results)):
            self.assertAlmostEqual(results[i] - task0res, i*period, delta=0.1)

    def test_timer_invalid(self):

        self.assertRaises(ValueError, self.timers.schedule, Task("invalid",g), 0)
            
    def test_timer_removal(self):
        n_timers = 4

        self.assertRaises(ValueError, self.timers.schedule, Task("invalid",g), 0)

        tasks = [ Task('task%d'%i, g) for i in range(n_timers) ]
        for i,task in enumerate(tasks):
            self.timers.schedule( task, i+1)

        time.sleep(3)

        for task in tasks:
            self.timers.remove_task( task.name )
            time.sleep(1)

        self.assertEqual(self.timers.num_active_tasks, 0)
        self.timers.stop()


    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass






