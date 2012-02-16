import unittest
import logging
import time
import random
from collections import defaultdict

from caa.tasks import Task, WorkersPool, TimersPool

logging.basicConfig(level=logging.DEBUG, format='[%(processName)s/%(threadName)s] %(asctime)s %(levelname)s: %(message)s')
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

    def test_workerspool_request(self):
        tasks = [ Task('task%d'%i, f, i+1,i+2,i+3) for i in range( self.workers.num_workers ) ]
        receipts = [ self.workers.request(task) for task in tasks ]
        
        results = wait_for_reqids(self.workers.get_result, receipts)

        for i,task in enumerate(results):
            self.assertEqual( i+1+i+2+i+3, task.result )

    def test_timerids(self):
        n_timers = 4

        tasks = [ Task('task%d'%i, g) for i in range(n_timers) ]
        timerids = [self.timers.schedule(task, i+1) for i,task in enumerate(tasks)]

        # periods go from 1 to n_timers

        sleep_for = 12  # lcm of 1,2,3,4
        time.sleep(sleep_for+0.2) 
        self.timers.stop() # implicit join

        for i,timerid in enumerate(timerids):
            reqids = self.timers.get_reqids(timerid)
            self.assertEqual( len(reqids), sleep_for//(i+1) )

    def test_timer_values(self):
        task = Task('task', g)
        period = random.randint(1,5)
        timerid = self.timers.schedule(task, period)

        time.sleep(period+0.2) 
        self.timers.stop() # implicit join

        reqids = self.timers.get_reqids(timerid)
        
        # get the values from the reqids associated to the timerid
        completed_tasks = [ self.workers.get_result(reqid) for reqid in reqids ]
 
        task0 = completed_tasks[0]
        for i in range(1,len(completed_tasks)):
            self.assertAlmostEqual(completed_tasks[i].result - task0.result, i, delta=0.01)

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






