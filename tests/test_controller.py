import pdb
import unittest
import logging

import pycassa
import pycassa.system_manager as SM

import mock

from caa import controller
from caa.controller import SubscriptionMode, ArchivedPV

from testing_settings import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('TestController')

class TestController(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.sm = SM.SystemManager()
        cls.sm.create_keyspace(KEYSPACE, replication_factor=1)

        cls.pool = pycassa.pool.ConnectionPool(KEYSPACE)

    def setUp(self):
        self.sm.create_column_family(KEYSPACE, CF)
        #self.sm.alter_column(KEYSPACE, CF, 'one', self.sm.INT_TYPE)
        self.controller = controller.Controller(KEYSPACE, CF)

    @mock.patch('epics.PV')
    def test_subscribe(self, mockPV):
        pv_mock_instance = mockPV.return_value

        self.controller.subscribe('FOO.PV')
        done_t = self.controller._subspool.done_queue.get()
        print( done_t, mockPV.call_args )

        self.assertIn( 'FOO.PV', self.controller.subscribed_pvs )
        self.assertIn( 'FOO.PV', self.controller.monitored_pvs )
 
#        self.controller.subscribe('BAR.PV', mode=SubscriptionMode.SCAN)
#        self.assertIn( 'BAR.PV', self.controller.subscribed_pvs )
#        self.assertIn( 'BAR.PV', self.controller.scanned_pvs)


    def test_get_info(self):
        self.controller.subscribe('DEFAULT.PV')
        av = self.controller.get_info('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0 )

        self.controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        av = self.controller.get_info('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0.123 )

        self.controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 
        av = self.controller.get_info('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode, SubscriptionMode.SCAN )
        self.assertEqual( av.scan_period, 123.1 )


    def test_subscrived_pvs(self):
        def random_word():
            from random import randint
            ini = ord('a')
            end = ord('z')
            length = randint(1,10)
            return ''.join( chr(randint(ini,end)) for _ in range(length) )

        some_names = set( random_word() for _ in range(10) )
        for name in some_names:
            self.controller.subscribe(name)

        sorted_names = sorted(some_names)
        self.assertEqual( sorted_names, self.controller.subscribed_pvs )

    def test_unsubscribe(self):
        self.controller.subscribe('FOO.PV')
        self.controller.unsubscribe('FOO.PV')
        self.assertNotIn( 'FOO.PV', self.controller.subscribed_pvs)
        self.assertNotIn( 'FOO.PV', self.controller.monitored_pvs )

    def test_save_config(self):
        import StringIO
        filelike = StringIO.StringIO()
        self.controller.subscribe('DEFAULT.PV')
        self.controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        self.controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 

        self.controller.save_config(filelike)
        logger.info("Saved config looks like:\n%s", filelike.getvalue())
        filelike.close()

    def test_load_config(self):
        import StringIO
        filelike = StringIO.StringIO()
        self.controller.subscribe('DEFAULT.PV')
        self.controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        self.controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 

        self.controller.save_config(filelike)
        filelike.seek(0)

        # reset it
        self.controller = controller.Controller(KEYSPACE, CF)

        self.controller.load_config(filelike)

        av = self.controller.get_info('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0 )

        self.controller.subscribe('FOO.PV', 
                mode=SubscriptionMode.MONITOR, monitor_delta=0.123)
        av = self.controller.get_info('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode, SubscriptionMode.MONITOR )
        self.assertEqual( av.monitor_delta, 0.123 )

        self.controller.subscribe('BAR.PV', 
                mode=SubscriptionMode.SCAN, scan_period=123.1) 
        av = self.controller.get_info('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode, SubscriptionMode.SCAN )
        self.assertEqual( av.scan_period, 123.1 )


    def tearDown(self):
        self.sm.drop_column_family(KEYSPACE,CF)
        self.controller.shutdown()
        

    @classmethod
    def tearDownClass(cls):
        cls.sm.drop_keyspace(KEYSPACE)
        cls.pool.dispose()






