import pdb
import unittest
import logging
import time
import datetime
import itertools
from collections import defaultdict

from caa.conf import settings, ENVIRONMENT_VARIABLE
import os
os.environ[ENVIRONMENT_VARIABLE] = 'caa.settings_dev'

from caa import controller, datastore, SubscriptionMode, ArchivedPV

logging.basicConfig(level=logging.INFO, 
        format='[%(processName)s/%(threadName)s] %(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('TestController')

NUM_RANDOM_PVS = 10

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

def _block_ioc(block):
    if block:
        print("Blocking IOC via iptables")
        action = '-A'
    else:
        print("Unblocking IOC via iptables")
        action = '-D'
    return os.system('sudo -A iptables %s OUTPUT -p tcp --sport 5064 -j REJECT' % action)

class TestController(unittest.TestCase):
 
    @classmethod
    def setUpClass(cls):
        _block_ioc(False)

    def setUp(self):
        reload(controller)
        controller.initialize(recreate=True)

        self.long_pvs = [ 'test:long%d' % i for i in range(1,NUM_RANDOM_PVS) ]
        self.double_pvs = [ 'test:double%d' % i for i in range(1,NUM_RANDOM_PVS) ]
        self.existing_pvs = self.long_pvs + self.double_pvs
        self.fake_pvs  = ['test:doesntexist%d'%i for i in range(5)]
        self.nonchanging_pvs = [ 'test:long0' ]

        self.pvs = self.existing_pvs + self.fake_pvs

    def test_get_statuses(self):
        # connect
        reqids = [controller.subscribe(pv, SubscriptionMode.Monitor()) for pv in self.pvs]
        wait_for_reqids(controller.get_result, reqids)
        
        conn_ts = datastore._get_timestamp_ms()
        # 1st status from initial connection => true
        #
        self.assertEqual(_block_ioc(True), 0)
        time.sleep(40)
        # 2nd status after disconnection from timeout => false
        #
        self.assertEqual(_block_ioc(False), 0)
        # 3rd status after unblocking IOC, reconnection => true
        time.sleep(2)


        for pv in self.existing_pvs: 
            statuses = controller.get_statuses(pv, limit=3)
            self.assertTrue(statuses)
            self.assertTrue(len(statuses), 3)

            [reconn, disconn, conn] = statuses

            self.assertTrue(conn.connected)
            self.assertLess( (conn.timestamp - conn_ts)/1e6, 0.5 )

            self.assertFalse(disconn.connected)
            self.assertTrue(reconn.connected)
            
        for fakepv in self.fake_pvs:
            statuses = controller.get_statuses(fakepv)
            self.assertEqual(statuses, [])

    def test_get_values(self):
        self.test_subscribe()

        # give it time to gather some data
        wait_for = 10
        ioc_freq = 10 # Hz
        time.sleep(wait_for) # ioc generates values at 1 Hz
        expected_num_values = wait_for * ioc_freq

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values, [])

        for longpv in self.long_pvs:
            values = controller.get_values(longpv, limit=expected_num_values)
            logger.info("Received %d values after %d seconds at %d Hz", len(values), wait_for, ioc_freq)
            self.assertEqual(len(values), expected_num_values)
            # we don't really know how for from a change the PV is. We 
            # can only guarantee that we'll get expected_num_values 
            # updates during our sleep period
            first_value = int(values[0]['value'])
            for (i,value) in enumerate(values):
                self.assertEqual(longpv, value['pvname'])
                self.assertEqual('long', value['type'])
                self.assertEqual(first_value-i, int(value['value']))

        for doublepv in self.double_pvs:
            values = controller.get_values(doublepv, limit=expected_num_values)
            logger.info("Received %d values after %d seconds at %d Hz", len(values), wait_for, ioc_freq)
            self.assertEqual(len(values), expected_num_values)
            first_value = float(values[0]['value'])
            for (i,value) in enumerate(values):
                self.assertEqual(doublepv, value['pvname'])
                self.assertEqual('double', value['type'])
                self.assertEqual(first_value-i, float(value['value']))

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)


    def test_get_values_by_date(self):
        self.test_subscribe()

        wait_for = 5
        start = datetime.datetime.now()
        halftime = start + datetime.timedelta(seconds=wait_for/2.0)
 
        time.sleep(wait_for) 

        # test "up to" semantics, open interval on the left
        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                all_values = controller.get_values(pv)
                upto_values = controller.get_values(pv, to_date=halftime) # first half
                from_values = controller.get_values(pv, from_date=halftime) # second half
                ts = time.mktime( halftime.timetuple())
                for v in upto_values:
                    self.assertLessEqual(v['timestamp'], ts)
                for v in from_values:
                    self.assertGreaterEqual(v['timestamp'], ts)

                # it's >= because there may be some overlap, due to the loss of precision in the
                # timestamp we use in the get_values call
                self.assertGreaterEqual( len(upto_values)+len(from_values), len(all_values) )

    def test_get_values_throttled(self):
        # give it time to gather some data
        wait_for = 5
        ioc_freq = 10 # Hz
        max_archiving_freq = 5 # Hz

        mode = SubscriptionMode.Monitor(max_freq=max_archiving_freq)
        self.test_subscribe(mode)

        time.sleep(wait_for) 

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values, [])

        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                values = controller.get_values(pv)
                logger.info("Received %d values for '%s' after %d seconds at %d Hz", \
                        len(values), pv, wait_for, max_archiving_freq)

                # diferences in timestamp shouldn't exceed 1/max_archiving_freq (period) 
                max_period = 1/max_archiving_freq
                for i in range(len(values)-1):
                    ith = values[i]['timestamp']
                    ip1th = values[i+1]['timestamp'] 
                    self.assertGreaterEqual( ith-ip1th, max_period) 

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)

    def test_get_values_delta(self):
        # give it time to gather some data
        wait_for = 10
        ioc_freq = 10 # Hz
        delta = 2

        mode = SubscriptionMode.Monitor(delta=delta)
        self.test_subscribe(mode)

        time.sleep(wait_for) 

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values, [])

        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                values = controller.get_values(pv)
                logger.info("Received %d values for '%s' after %d seconds", \
                        len(values), pv, wait_for)
                for i in range(len(values)-1):
                    ith = values[i]['value']
                    ip1th = values[i+1]['value'] 
                    self.assertGreaterEqual( ith-ip1th, delta) 

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)

    def test_get_values_disconnected(self):
        conn_ts = datastore._get_timestamp_ms()
        reqids = [ controller.subscribe(pv, SubscriptionMode.Monitor()) \
                for pv in self.existing_pvs ]
        results = wait_for_reqids(controller.get_result, reqids)
        time.sleep(2)
        # when a pv gets disconnected, NaN should be stored as its value. 
        # moreover, the pv's status should reflect the disconnection at that
        # point in time
        for pv in self.existing_pvs:
            statuses = controller.get_statuses(pv)
            self.assertTrue(statuses)
            status = statuses[0]
            self.assertTrue(status.connected)

            last_values = controller.get_values(pv)
            self.assertTrue(last_values)
            last_value = last_values[0]
            self.assertGreater(last_value['archived_at_ts'], conn_ts)
            self.assertGreater(last_value['value'], 1)


        predisconn_ts = datastore._get_timestamp_ms()
        _block_ioc(True)
        time.sleep(35)
        postdisconn_ts = datastore._get_timestamp_ms()

        for pv in self.existing_pvs:
            statuses = controller.get_statuses(pv)
            self.assertTrue(statuses)
            status = statuses[0]
            self.assertFalse(status.connected)

            last_values = controller.get_values(pv)
            self.assertTrue(last_values)
            last_value = last_values[0]
            self.assertGreater(last_value['archived_at_ts'], predisconn_ts)
            self.assertGreater(postdisconn_ts, last_value['archived_at_ts'])
            self.assertIsNone(last_value['value'])

        _block_ioc(False)


    def test_subscribe(self, mode=SubscriptionMode.Monitor()):
        reqids = [ controller.subscribe(pv, mode) for pv in self.pvs ]
        results = wait_for_reqids(controller.get_result, reqids)
        
        apvs = [ controller.get_info(pv) for pv in self.pvs ]
        for apv in apvs:
            self.assertIn(apv.name, self.pvs)

        pvnames_from_receipts = [ task.name for task in results ]
        for pv in self.pvs:
            self.assertIn(pv, pvnames_from_receipts)

        for task in results:
            self.assertTrue(task.result)

    def test_subscribe_scan(self):
        for pv in self.pvs[:4]:
            self.assertEqual(len(controller.get_values(pv)), 0)

        periods = (1,1.5, 2, 2.1)
        for pv,p in zip(self.pvs[:4], periods):
            controller.subscribe(pv, SubscriptionMode.Scan(period=p))

        sleep_for = 10
        time.sleep(sleep_for)
        
        for pv,p in zip(self.pvs[:4], periods):
            res = controller.get_values(pv)
            self.assertAlmostEqual(sleep_for/p, len(res), delta=1)

    def test_constant_value_scan(self):
        pv = self.nonchanging_pvs[0]
        controller.subscribe(pv, SubscriptionMode.Scan(period=1))

        time.sleep(4)

        values = controller.get_values(self.nonchanging_pvs[0])
        self.assertGreaterEqual(len(values), 3)
        self.assertLessEqual(len(values), 4)
        last_value = values[-1] # ie, oldest
        for i,value in enumerate(reversed(values[:-1])):
            self.assertAlmostEqual(value['archived_at_ts']-(i+1)*1e6, last_value['archived_at_ts'], delta=0.1*1e6)
            self.assertEqual(value['timestamp'], last_value['timestamp'])
        

    def test_unsubscribe_monitor(self):
        controller.subscribe(self.long_pvs[0], SubscriptionMode.Monitor())

        #check that the system has registered the subscription
        apv = controller.get_info(self.long_pvs[0])
        self.assertEqual( apv.name, self.long_pvs[0] )

        # wait for a while so that we gather some data 
        time.sleep(2)

        # request unsubscription
        self.assertTrue(controller.unsubscribe(self.long_pvs[0]))
        # take note of the data present
        before = controller.get_values(self.long_pvs[0])

        # verify that the system has removed the pv
        info = controller.get_info(self.long_pvs[0])
        self.assertEqual(None, info)

        # wait to see if we are still receiving data 
        time.sleep(2)
        after = controller.get_values(self.long_pvs[0])

        # we shouldn't have received any data during the last sleep period
        self.assertEqual(len(before), len(after))

    def test_unsubscribe_unknown(self):
        self.assertEqual([], controller.unsubscribe('foobar'))

    def test_unsubscribe_scan(self):
        controller.subscribe(self.long_pvs[0], SubscriptionMode.Scan(period=1))

        #check that the system has registered the subscription
        apv = controller.get_info(self.long_pvs[0])
        self.assertEqual( apv.name, self.long_pvs[0] )

        # wait for a while so that we gather some data 
        time.sleep(3)

        # request unsubscription
        self.assertTrue(controller.unsubscribe(self.long_pvs[0]))
        before = controller.get_values(self.long_pvs[0])

        # verify that the system has removed the pv
        info = controller.get_info(self.long_pvs[0])
        self.assertEqual(None, info)

        # wait to see if we are still receiving data 
        time.sleep(3)
        after = controller.get_values(self.long_pvs[0])

        # we shouldn't have received any data during the last sleep period
        self.assertEqual(len(before), len(after))

        # and the scan task's timer should have been cleaned up
        self.assertEqual(controller.timers.num_active_tasks, 0)


    def test_get_info(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor())
        apv = controller.get_info('DEFAULT.PV')
        self.assertEqual( apv.name, 'DEFAULT.PV' )
        self.assertEqual( apv.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( apv.mode.delta, 0.0 )

        controller.subscribe('FOO.PV', SubscriptionMode.Monitor(delta=0.123))
        apv = controller.get_info('FOO.PV')
        self.assertEqual( apv.name, 'FOO.PV' )
        self.assertEqual( apv.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( apv.mode.delta, 0.123 )

        controller.subscribe('BAR.PV', SubscriptionMode.Scan(period=123.1) )
        apv = controller.get_info('BAR.PV')
        self.assertEqual( apv.name, 'BAR.PV' )
        self.assertEqual( apv.mode.name , SubscriptionMode.Scan.name )
        self.assertEqual( apv.mode.period, 123.1 )

        info = controller.get_info('whatever')
        self.assertEqual(None, info)

    def test_save_config(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor())
        controller.subscribe('FOO.PV',  SubscriptionMode.Monitor(0.123))
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(123.1)) 

        cfg = controller.save_config()
        logger.info("Saved config looks like:\n%s", cfg)

    def test_load_config(self):
        pvnames = ('DEFAULT.PV', 'FOO.PV', 'BAR.PV')
        
        reqids = []
        reqids.append(controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor()))
        reqids.append(controller.subscribe('FOO.PV',  SubscriptionMode.Monitor(0.123)))
        reqids.append(controller.subscribe('BAR.PV', SubscriptionMode.Scan(123.1)))

        wait_for_reqids(controller.get_result, reqids)

        cfg = controller.save_config()

        reqids = controller.unsubscribe('*')
        wait_for_reqids(controller.get_result, reqids)

        logger.info("Trying to load:\n%s", cfg)

        reqids = controller.load_config(cfg)
        results = wait_for_reqids(controller.get_result, reqids)

        pvnames_from_receipts = [ task.name for task in results ]

        self.assertIn('DEFAULT.PV', pvnames_from_receipts)
        self.assertIn('FOO.PV', pvnames_from_receipts)
        self.assertIn('BAR.PV', pvnames_from_receipts)

        av = controller.get_info('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( av.mode.delta, 0.0 )

        av = controller.get_info('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( av.mode.delta, 0.123 )

        av = controller.get_info('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Scan.name )
        self.assertEqual( av.mode.period, 123.1 )


    def test_get_pvs(self):
        monitor_mode = SubscriptionMode.Monitor()
        scan_mode = SubscriptionMode.Scan(period=1)
        now_ts = time.time() * 1e6
        num_pvs = len(self.pvs)
        half = num_pvs // 2

        receipts = [ controller.subscribe(pv, monitor_mode) for pv in self.pvs[:half] ]
        receipts += [ controller.subscribe(pv, scan_mode) for pv in self.pvs[half:] ]

        wait_for_reqids(controller.get_result, receipts)
            
        time.sleep(5)

        all_apvs = [ apv for apv in controller.get_pvs() ]
        all_pv_names = [ apv.name for apv in all_apvs ]
        all_pv_modes = [ apv.mode for apv in all_apvs ]
        all_pv_sinces = [ apv.since for apv in all_apvs ]

        for pv in self.pvs:
            self.assertIn(pv, all_pv_names)

        for mode in all_pv_modes[:half]:
            self.assertEqual(monitor_mode, mode)
        for mode in all_pv_modes[half:]:
            self.assertEqual(scan_mode, mode)

        for since in all_pv_sinces:
            self.assertAlmostEqual(now_ts, since, delta=10e6)


        long_apvs = [apv for apv in controller.get_pvs('*long*')]
        for long_apv in long_apvs:
            self.assertRegexpMatches(long_apv.name, '.*long.*')

        scan_apvs = [apv for apv in controller.get_pvs(modename_pattern='Scan')]
        monitor_apvs = [apv for apv in controller.get_pvs(modename_pattern='Monitor')]

        for scan_apv in scan_apvs:
            self.assertEqual(scan_apv.mode.name, SubscriptionMode.Scan.name)

        for monitor_apv in monitor_apvs:
            self.assertEqual(monitor_apv.mode.name, SubscriptionMode.Monitor.name)



    def tearDown(self):
        try:
            for pv in self.pvs:
                controller.unsubscribe(pv)
        except ValueError:
            pass

        controller.shutdown()
        #datastore.reset_schema(settings.DATASTORE['servers'][0], settings.DATASTORE['keyspace'])

    @classmethod
    def tearDownClass(cls):
        _block_ioc(False)






