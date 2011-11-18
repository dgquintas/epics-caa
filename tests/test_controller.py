import unittest
import logging
import time
from pprint import pprint
from collections import defaultdict

import pycassa
import pycassa.system_manager as SM

from caa import controller, datastore, SubscriptionMode, ArchivedPV, config

logging.basicConfig(level=logging.INFO)
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
        for fakepv in self.fake_pvs:
            controller.subscribe(fakepv)
        for pv in self.existing_pvs:
            controller.subscribe(pv)

        results = [ controller.get_result(timeout=self.timeout) 
                    for _ in range(len(controller.subscriptions)) ]
        self.assertEqual( len(results), len(controller.subscriptions) )

        status = controller.get_status(self.pvs)
        for fakepv in self.fake_pvs:
            self.assertIn(fakepv, status)
            status_data = status[fakepv]
            self.assertFalse(status_data['connected'])
        for pv in self.existing_pvs: 
            self.assertIn(pv, status)
            status_data = status[pv]
            self.assertTrue(status_data['connected'])

        status = controller.get_status(['whatever'])
        status_data = status['whatever']
        self.assertEqual(status_data, {})


        # wait for ttl seconds to verify that values are actually getting expired
        ttl = config.DATASTORE['status_ttl']
        time.sleep(ttl+1)
        status = controller.get_status(self.pvs)
        for pv, data in status.iteritems():
            self.assertEqual(data, {})

    def test_get_values(self):
        self.test_subscribe()

        # give it time to gather some data
        wait_for = 2
        ioc_freq = 10 # Hz
        time.sleep(wait_for) # ioc generates values at 1 Hz
        min_expected_num_values = wait_for * ioc_freq

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values, [])

        for longpv in self.long_pvs:
            values = controller.get_values(longpv)
            self.assertGreaterEqual(len(values), min_expected_num_values)
            # we don't really know how for from a change the PV is. We 
            # can only guarantee that we'll get min_expected_num_values 
            # updates during our sleep period
            first_value = int(values[0]['value'])
            for (i,value) in enumerate(values):
                self.assertEqual(longpv, value['pv'])
                self.assertEqual('long', value['value_type'])
                self.assertEqual(first_value+i, int(value['value']))

        for doublepv in self.double_pvs:
            values = controller.get_values(doublepv)
            self.assertGreaterEqual(len(values), min_expected_num_values)
            # we don't really know how for from a change the PV is. We 
            # can only guarantee that we'll get min_expected_num_values 
            # updates during our sleep period
            first_value = float(values[0]['value'])
            for (i,value) in enumerate(values):
                self.assertEqual(doublepv, value['pv'])
                self.assertEqual('double', value['value_type'])
                self.assertEqual(first_value+i, float(value['value']))

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            self.assertEqual([], values)
    def test_subscribe(self):
        receipts = [ controller.subscribe(pv) for pv in self.pvs ]

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

        #TODO: test for scanned pvs
   

    def test_unsubscribe(self):
        controller.subscribe(self.long_pvs[0])
        apv = controller.get_info(self.long_pvs[0])
        self.assertEqual( apv.name, self.long_pvs[0] )
        time.sleep(2)
        before = controller.get_values(self.long_pvs[0])
        controller.unsubscribe(self.long_pvs[0])
        apv = controller.get_info(self.long_pvs[0])
        self.assertEqual(None, apv)
        time.sleep(2)
        after = controller.get_values(self.long_pvs[0])
        self.assertEqual(len(before), len(after))


    def test_get_info(self):
        controller.subscribe('DEFAULT.PV')
        av = controller.get_info('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0 )

        controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        av = controller.get_info('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0.123 )

        controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 
        av = controller.get_info('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode, SubscriptionMode.SCAN )
        self.assertEqual( av.scan_period, 123.1 )

        av = controller.get_info('whatever')
        self.assertEqual(None, av)

    def test_save_config(self):
        import StringIO
        filelike = StringIO.StringIO()
        controller.subscribe('DEFAULT.PV')
        controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 

        controller.save_config(filelike)
        logger.info("Saved config looks like:\n%s", filelike.getvalue())
        filelike.close()

    def test_load_config(self):
        import StringIO
        filelike = StringIO.StringIO()
        controller.subscribe('DEFAULT.PV')
        controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 

        controller.save_config(filelike)
        filelike.seek(0)

        # reset it
        reload(controller)
        
        self.assertFalse(controller.subscriptions)

        controller.load_config(filelike)
        done_receipts = (controller.get_result(timeout=self.timeout) \
                for _ in range(len(controller.subscriptions)) )

        pvnames_from_receipts = [ task.args[0].name for (_,task) in done_receipts ]

        self.assertIn('DEFAULT.PV', pvnames_from_receipts)
        self.assertIn('FOO.PV', pvnames_from_receipts)
        self.assertIn('BAR.PV', pvnames_from_receipts)

        av = controller.get_info('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0 )

        av = controller.get_info('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0.123 )

        av = controller.get_info('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode, SubscriptionMode.SCAN )
        self.assertEqual( av.scan_period, 123.1 )


    def tearDown(self):
        controller.shutdown()
        time.sleep(1)
        datastore.reset_schema(config.DATASTORE['servers'][0], config.DATASTORE['keyspace'])

    @classmethod
    def tearDownClass(cls):
        pass






