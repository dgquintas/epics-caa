import unittest
import logging
import time
import datetime
from pprint import pprint
from collections import defaultdict

from caa import controller, datastore, SubscriptionMode, ArchivedPV, config

logging.basicConfig(level=logging.INFO, format='[%(processName)s/%(threadName)s] %(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('TestController')

NUM_RANDOM_PVS = 10

class TestController(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        reload(controller)
        datastore.create_schema(config.DATASTORE['servers'][0], config.DATASTORE['keyspace'], recreate=True)

        self.long_pvs = [ 'test:long%d' % i for i in range(1,NUM_RANDOM_PVS) ]
        self.double_pvs = [ 'test:double%d' % i for i in range(1,NUM_RANDOM_PVS) ]
        self.existing_pvs = self.long_pvs + self.double_pvs
        self.fake_pvs  = ['test:doesntexist']

        self.pvs = self.existing_pvs + self.fake_pvs

        self.timeout = config.CONTROLLER['epics_connection_timeout']*1.5 #for good measure

    def test_get_status(self):
        for pv in self.pvs:
            controller.subscribe(pv, SubscriptionMode.Monitor())

        results = [ controller.get_result(timeout=self.timeout) 
                    for _ in range(len(self.pvs)) ]
        self.assertEqual( len(results), len(self.pvs) )

        for pv in self.existing_pvs: 
            status = controller.get_status(pv)
            self.assertTrue(status)
            self.assertTrue(status['connected'], "Failed for %s" % pv)

        for fakepv in self.fake_pvs:
            status = controller.get_status(fakepv)
            self.assertEqual(status, {})

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
                self.assertEqual(first_value+i, int(value['value']))

        for doublepv in self.double_pvs:
            values = controller.get_values(doublepv, limit=expected_num_values)
            logger.info("Received %d values after %d seconds at %d Hz", len(values), wait_for, ioc_freq)
            self.assertEqual(len(values), expected_num_values)
            first_value = float(values[0]['value'])
            for (i,value) in enumerate(values):
                self.assertEqual(doublepv, value['pvname'])
                self.assertEqual('double', value['type'])
                self.assertEqual(first_value+i, float(value['value']))

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
                    self.assertGreaterEqual( ip1th-ith, max_period) 

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)

    def test_get_values_delta(self):
        # give it time to gather some data
        wait_for = 5
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
                    self.assertGreaterEqual( ip1th-ith, delta) 

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)

    def test_subscribe(self, mode=SubscriptionMode.Monitor()):
        receipts = [ controller.subscribe(pv, mode) for pv in self.pvs ]

        done_receipts = dict(controller.get_result(timeout=self.timeout) \
                for _ in self.pvs )

        apvs = [ controller.get_info(pv) for pv in self.pvs ]
        for apv in apvs:
            self.assertIn(apv.name, self.pvs)

        for receipt in receipts:
            self.assertIn(receipt, done_receipts)

        pvnames_from_receipts = [ subreq.name for subreq in done_receipts.itervalues() ]
        for pv in self.pvs:
            self.assertIn(pv, pvnames_from_receipts)

        for completed_req in done_receipts.itervalues():
            self.assertTrue(completed_req.result)

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

    def test_unsubscribe(self):
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

        # unsubscribing to an unknown pv
        self.assertFalse(controller.unsubscribe('foobar'))


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
        import StringIO
        filelike = StringIO.StringIO()
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor())
        controller.subscribe('FOO.PV',  SubscriptionMode.Monitor(0.123))
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(123.1)) 

        controller.save_config(filelike)
        logger.info("Saved config looks like:\n%s", filelike.getvalue())
        filelike.close()

    def test_load_config(self):
        import StringIO
        filelike = StringIO.StringIO()
        lines = ( 
            '["DEFAULT.PV", {"mode": "Monitor", "delta": 0.0}, 1322529100410510]\n',
            '["FOO.PV", {"mode": "Monitor", "delta": 0.123}, 1322529100425237]\n',
            '["BAR.PV", {"mode": "Scan", "period": 123.1}, 1322529100431136]\n'
        )
        filelike.writelines(lines)
        filelike.seek(0)
        logger.info("Trying to load: %s", filelike.read())
        filelike.seek(0)

        controller.load_config(filelike)
        done_receipts = (controller.get_result(timeout=self.timeout) \
                for _ in range(len(lines)) )

        pvnames_from_receipts = [ task.args[0] for (_,task) in done_receipts ]

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

        filelike.close()

    def test_list_pvs(self):
        monitor_mode = SubscriptionMode.Monitor()
        scan_mode = SubscriptionMode.Scan(period=1)
        now_ts = time.time() * 1e6
        num_pvs = len(self.pvs)
        half = num_pvs // 2

        receipts = [ controller.subscribe(pv, monitor_mode) for pv in self.pvs[:half] ]
        receipts += [ controller.subscribe(pv, scan_mode) for pv in self.pvs[half:] ]

        for _ in self.pvs:
            controller.get_result(timeout=self.timeout)
    
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


    def tearDown(self):
        for pv in self.pvs:
            controller.unsubscribe(pv)

        controller.shutdown()
        time.sleep(1)
        datastore.reset_schema(config.DATASTORE['servers'][0], config.DATASTORE['keyspace'])

    @classmethod
    def tearDownClass(cls):
        pass






