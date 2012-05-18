import unittest
import random
import logging
import time
import datetime
import operator
import itertools

from conf import ENVIRONMENT_VARIABLE

import os
os.environ[ENVIRONMENT_VARIABLE] = 'settings_dev'

from caa import controller, datastore, SubscriptionMode

logging.basicConfig(level=logging.INFO, 
        format='[%(processName)s/%(threadName)s] %(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('TestController')

NUM_RANDOM_PVS = 10

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
        futures = [controller.subscribe(pv, SubscriptionMode.Monitor(delta=0.01)) for pv in self.pvs]
        [ fut.wait() for fut in futures ]
        
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

            self.assertTrue(conn['connected'])
            self.assertLess( (conn['timestamp'] - conn_ts)/1e6, 0.5 )

            self.assertFalse(disconn['connected'])
            self.assertTrue(reconn['connected'])

            self.assertEqual(reconn['pvname'], pv)
            self.assertEqual(conn['pvname'], pv)
            self.assertEqual(disconn['pvname'], pv)
            
        for fakepv in self.fake_pvs:
            statuses = controller.get_statuses(fakepv)
            # they must be there, with 'connected' set to false
            for status in statuses:
                self.assertFalse(status['connected'])

    def test_get_values(self):
        self.test_subscribe()

        # give it time to gather some data
        wait_for = 10
        ioc_freq = 10 # Hz
        time.sleep(wait_for) # ioc generates values at 1 Hz
        expected_num_values = wait_for * ioc_freq

        # doesn't exist AND never subscribed
        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values['rows'], [])
        self.assertEqual(no_values['meta']['total'], 0)

        for longpv in self.long_pvs:
            values = controller.get_values(longpv, limit=expected_num_values)
            rows = values['rows']
            logger.info("Received %d values after %d seconds at %d Hz", len(rows), wait_for, ioc_freq)
            self.assertEqual(len(rows), expected_num_values)
            # we don't really know how for from a change the PV is. We 
            # can only guarantee that we'll get expected_num_values 
            # updates during our sleep period
            first_value = int(rows[0]['value'])
            for (i,value) in enumerate(rows):
                self.assertEqual(longpv, value['pvname'])
                self.assertEqual('long', value['type'])
                self.assertEqual(first_value-i, int(value['value']))

        for doublepv in self.double_pvs:
            values = controller.get_values(doublepv, limit=expected_num_values)
            rows = values['rows']
            logger.info("Received %d values after %d seconds at %d Hz", len(rows), wait_for, ioc_freq)
            self.assertEqual(len(rows), expected_num_values)
            first_value = float(rows[0]['value'])
            print(first_value)
            for (i,value) in enumerate(rows):
                self.assertEqual(doublepv, value['pvname'])
                self.assertEqual('double', value['type'])
                self.assertEqual(first_value-i, float(value['value']))

        for fakepv in self.fake_pvs:
            values = controller.get_values(fakepv)
            # they don't exist but we subscribed to them. Expect entries with null values
            rows = values['rows']
            for row in rows:
                self.assertIsNone(row['value'])


    def test_get_values_by_date(self):
        self.test_subscribe()

        wait_for = 5
        start = datetime.datetime.now()
        halftime = start + datetime.timedelta(seconds=wait_for/2.0)
        halftime_ts = time.mktime( halftime.timetuple())
 
        time.sleep(wait_for) 

        # test "up to" semantics, open interval on the left
        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                all_values = controller.get_values(pv, limit=2**30)
                upto_values = controller.get_values(pv, to_date=halftime, limit=2**30) # first half
                from_values = controller.get_values(pv, from_date=halftime, limit=2**30) # second half
                ts = time.mktime( halftime.timetuple())
                for v in upto_values['rows']:
                    self.assertLessEqual(v['timestamp'], ts)
                for v in from_values['rows']:
                    self.assertGreaterEqual(v['timestamp'], ts)

                # it may happen that new values arrive between the gathering of values and the
                # counting colums operation
                self.assertLessEqual(len(all_values['rows']), all_values['meta']['total'])
                self.assertLessEqual(len(upto_values['rows']), upto_values['meta']['total'])
                self.assertLessEqual(upto_values['meta']['last_ts'], halftime_ts)
                self.assertGreaterEqual(from_values['meta']['first_ts'], halftime_ts)
                self.assertLessEqual(len(from_values['rows']), from_values['meta']['total'])


                # it's >= because there may be some overlap, due to the loss of precision in the
                # timestamp we use in the get_values call
                self.assertGreaterEqual( len(upto_values['rows'])+len(from_values['rows']), len(all_values['rows']) )

    def test_get_values_throttled(self):
        # give it time to gather some data
        wait_for = 5
        ioc_freq = 10 # Hz
        max_archiving_freq = 5 # Hz

        mode = SubscriptionMode.Monitor(delta=0.01,max_freq=max_archiving_freq)
        self.test_subscribe(mode)

        time.sleep(wait_for) 

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values['rows'], [])

        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                values = controller.get_values(pv)
                logger.info("Received %d values for '%s' after %d seconds at %d Hz", \
                        len(values['rows']), pv, wait_for, max_archiving_freq)

                # diferences in timestamp shouldn't exceed 1/max_archiving_freq (period) 
                max_period = 1/max_archiving_freq
                for i in range(len(values['rows'])-1):
                    ith = values['rows'][i]['timestamp']
                    ip1th = values['rows'][i+1]['timestamp'] 
                    self.assertGreaterEqual( ith-ip1th, max_period) 

    def test_get_values_delta(self):
        # give it time to gather some data
        wait_for = 10
        ioc_freq = 10 # Hz
        delta = 2

        mode = SubscriptionMode.Monitor(delta=delta)
        self.test_subscribe(mode)

        time.sleep(wait_for) 

        no_values = controller.get_values('test:doesntexist')
        self.assertEqual(no_values['rows'], [])

        for pvset in (self.long_pvs, self.double_pvs):
            for pv in pvset:
                values = controller.get_values(pv)
                rows = values['rows']
                logger.info("Received %d values for '%s' after %d seconds", \
                        len(rows), pv, wait_for)
                for i in range(len(rows)-1):
                    ith = rows[i]['value']
                    ip1th = rows[i+1]['value'] 
                    self.assertGreaterEqual( ith-ip1th, delta) 

    def test_get_values_disconnected(self):
        conn_ts = datastore._get_timestamp_ms()
        futures = [ controller.subscribe(pv, SubscriptionMode.Monitor(delta=0.01)) \
                     for pv in self.existing_pvs ]
        results = [ fut.get() for fut in futures ]
        time.sleep(2)
        # when a pv gets disconnected, NaN should be stored as its value. 
        # moreover, the pv's status should reflect the disconnection at that
        # point in time
        for pv in self.existing_pvs:
            statuses = controller.get_statuses(pv)
            self.assertTrue(statuses)
            status = statuses[0]
            self.assertTrue(status['connected'])

            last_values = controller.get_values(pv)
            self.assertTrue(last_values['rows'])
            last_value = last_values['rows'][0]
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
            self.assertFalse(status['connected'])

            last_values = controller.get_values(pv)
            self.assertTrue(last_values['rows'])
            last_value = last_values['rows'][0]
            self.assertGreater(last_value['archived_at_ts'], predisconn_ts)
            self.assertGreater(postdisconn_ts, last_value['archived_at_ts'])
            self.assertIsNone(last_value['value'])

        _block_ioc(False)


    def test_subscribe(self, mode = None):
        mode = mode or SubscriptionMode.Monitor(delta=0.01)
        futures = [ controller.subscribe(pv, mode) for pv in self.pvs ]
        results = [fut.get() for fut in futures]
        
        self.assertTrue(all(results))

        pvs = dict(controller.list_subscribed())
        for pv in self.pvs:
            self.assertIn(pv, pvs)

    def test_msubscribe(self ):
        mode_choices = [SubscriptionMode.Monitor(delta=0.01), SubscriptionMode.Scan(period=1)]
        modes = [random.choice(mode_choices) for _ in self.pvs]
        futures = controller.msubscribe( self.pvs, modes )

        results = [ fut.get() for fut in futures ]
        
        self.assertTrue(all(results))
        pvs = dict(controller.list_subscribed())
        for pv in self.pvs:
            self.assertIn(pv, pvs)

    def test_subscribe_resubscribe(self):
        # subscribe to an already subscribed pv, with a different mode perhaps
        pv = self.long_pvs[0]
        controller.subscribe(pv, SubscriptionMode.Scan(period=1))
        apv = controller.get_apv(pv)
        self.assertEqual( apv.mode.period, 1)

        controller.subscribe(pv, SubscriptionMode.Scan(period=2))
        apv = controller.get_apv(pv)
        self.assertEqual( apv.mode.period, 2)

    def test_subscribe_scan(self):
        for pv in self.pvs[:4]:
            self.assertEqual(len(controller.get_values(pv)['rows']), 0)

        periods = (1,1.5, 2, 2.1)
        for pv,p in zip(self.pvs[:4], periods):
            controller.subscribe(pv, SubscriptionMode.Scan(period=p))

        sleep_for = 10
        time.sleep(sleep_for)
        
        for pv,p in zip(self.pvs[:4], periods):
            res = controller.get_values(pv)['rows']
            self.assertAlmostEqual(sleep_for/p, len(res), delta=1)

    def test_constant_value_scan(self):
        pv = self.nonchanging_pvs[0]
        controller.subscribe(pv, SubscriptionMode.Scan(period=1))

        time.sleep(4)

        values = controller.get_values(self.nonchanging_pvs[0])
        rows = values['rows']
        self.assertGreaterEqual(len(rows), 3)
        self.assertLessEqual(len(rows), 4)
        last_value = rows[-1] # ie, oldest
        for i,value in enumerate(reversed(rows[:-1])):
            self.assertAlmostEqual(value['archived_at_ts']-(i+1)*1e6, last_value['archived_at_ts'], delta=0.1*1e6)
            self.assertEqual(value['timestamp'], last_value['timestamp'])
        

    def test_unsubscribe_monitor(self):
        pvname = self.long_pvs[0]
        controller.subscribe(pvname, SubscriptionMode.Monitor(delta=0.01))

        #check that the system has registered the subscription
        apv = controller.get_apv(pvname)
        self.assertEqual( apv.name, self.long_pvs[0] )

        # wait for a while so that we gather some data 
        time.sleep(2)

        # request unsubscription
        unsub_req = controller.unsubscribe(self.long_pvs[0])
        self.assertTrue(unsub_req)
        unsub_req.wait()
        # take note of the data present
        before = controller.get_values(self.long_pvs[0])['rows']

        # verify that the system has removed the pv
        apv = controller.get_apv(self.long_pvs[0])
        self.assertFalse(apv.subscribed)

        # wait to see if we are still receiving data 
        time.sleep(2)
        after = controller.get_values(self.long_pvs[0])['rows']

        # we shouldn't have received any data during the last sleep period
        self.assertEqual(len(before), len(after))

    def test_unsubscribe_misc(self):
        # unsubscribe from an unknown pv
        self.assertIsNone(controller.unsubscribe('foobar')) 

    def test_munsubscribe(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01))
        controller.subscribe('FOO.PV', SubscriptionMode.Monitor(delta=0.123))
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(period=123.1) )
        
        pvs = dict(controller.list_subscribed())
        self.assertEqual( sorted(pvs), ['BAR.PV', 'DEFAULT.PV', 'FOO.PV'])

        futures = controller.munsubscribe(pvs)
        [ fut.wait() for fut in futures]
        
        pvs = dict(controller.list_subscribed())
        self.assertEqual(pvs, {})

        # munsubscribe providing the wrong type or argument (not a sequence)
        self.assertRaises(TypeError, controller.munsubscribe, 'foobar')

    def test_unsubscribe_scan(self):
        controller.subscribe(self.long_pvs[0], SubscriptionMode.Scan(period=1))

        #check that the system has registered the subscription
        apv = controller.get_apv(self.long_pvs[0])
        self.assertEqual( apv.name, self.long_pvs[0] )

        # wait for a while so that we gather some data 
        time.sleep(3)

        # request unsubscription
        unsub_req = controller.unsubscribe(self.long_pvs[0])
        self.assertTrue(unsub_req)
        unsub_req.wait()
        before = controller.get_values(self.long_pvs[0])

        # verify that the system has removed the pv
        apv = controller.get_apv(self.long_pvs[0])
        self.assertFalse(apv.subscribed)

        # wait to see if we are still receiving data 
        time.sleep(3)
        after = controller.get_values(self.long_pvs[0])

        # we shouldn't have received any data during the last sleep period
        self.assertEqual(len(before), len(after))

        # and the scan task's timer should have been cleaned up
        self.assertEqual(controller.timers.num_active_tasks, 0)

    def test_list_pvs_simple(self):
        def just_the_name(list_of_pairs):
            return map(operator.itemgetter(0), list_of_pairs)

        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01)).wait()
        controller.subscribe('FOO.PV', SubscriptionMode.Monitor(delta=0.123)).wait()
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(period=123.1) ).wait()
        
        pvs = just_the_name(controller.list_subscribed())
        self.assertItemsEqual( pvs, ['BAR.PV', 'DEFAULT.PV', 'FOO.PV'])

        scan_pvs = just_the_name(controller.list_by_mode(SubscriptionMode.Scan.name))
        self.assertEqual( scan_pvs, ['BAR.PV'])

        monitor_pvs = just_the_name(controller.list_by_mode(SubscriptionMode.Monitor.name))
        self.assertEqual( monitor_pvs, ['DEFAULT.PV', 'FOO.PV'])
    
    def test_list_archived_only(self):
        controller.subscribe('test:long1', SubscriptionMode.Monitor(delta=0.01)).wait()
        controller.subscribe('test:long2', SubscriptionMode.Monitor(delta=0.123)).wait()
        controller.subscribe('test:long3', SubscriptionMode.Scan(period=123.1) ).wait()
    
        time.sleep(5)

        controller.unsubscribe('test:long1').wait()
        controller.unsubscribe('test:long2').wait()
        # still subscribed to test:long3

        archived = dict(controller.list_archived(False)) #don't include currently subscribed
        self.assertItemsEqual(archived.keys(), ['test:long1', 'test:long2'])


    def test_list_subscribed_only(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01)).wait()
        controller.subscribe('FOO.PV', SubscriptionMode.Monitor(delta=0.123)).wait()
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(period=123.1) ).wait()

        subscribed = dict(controller.list_subscribed())
        self.assertItemsEqual(subscribed.keys(), ['BAR.PV', 'DEFAULT.PV', 'FOO.PV'])

        controller.unsubscribe('FOO.PV').wait()
        
        subscribed = dict(controller.list_subscribed())
        self.assertItemsEqual(subscribed.keys(), ['BAR.PV', 'DEFAULT.PV'])

        # yet 'FOO.PV' is still present in the archive
        arched = dict(controller.list_archived(False))
        self.assertItemsEqual(arched.keys(), ['FOO.PV'])

    def test_list_and_get_pvs(self):
        monitor_mode = SubscriptionMode.Monitor(delta=0.01)
        scan_mode = SubscriptionMode.Scan(period=1)
        now_ts = time.time() * 1e6
        num_pvs = len(self.pvs)
        half = num_pvs // 2

        monitored_pvs = self.pvs[:half]
        scanned_pvs = self.pvs[half:]
        futures = [ controller.subscribe(pv, monitor_mode) for pv in monitored_pvs ]
        futures += [ controller.subscribe(pv, scan_mode) for pv in scanned_pvs ]

        [ fut.wait() for fut in futures ] 
            
        time.sleep(5)
    
        pvs_dict = dict(controller.list_archived())

        for pv in self.pvs:
            self.assertIn(pv, pvs_dict)
            self.assertEqual( pvs_dict[pv].name, pv )

            if pv in monitored_pvs:
                self.assertEqual( pvs_dict[pv].mode.mode, monitor_mode.name )
            if pv in scanned_pvs:
                self.assertEqual( pvs_dict[pv].mode.mode,scan_mode.name )
            
            self.assertAlmostEqual(now_ts, pvs_dict[pv].since, delta=10e6)

        scanpvs = dict(controller.list_by_mode(scan_mode.name))
        for pv in scanpvs:
            self.assertEqual( scanpvs[pv].mode.mode, scan_mode.name) 

        monitorpvs = dict(controller.list_by_mode(monitor_mode.name))
        for pv in monitorpvs:
            self.assertEqual( monitorpvs[pv].mode.mode, monitor_mode.name) 


    def test_get_apvs(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01))
        controller.subscribe('FOO.PV', SubscriptionMode.Monitor(delta=0.123))
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(period=123.1) )

        pvnames = ('DEFAULT.PV', 'FOO.PV', 'BAR.PV')
        all_apvs = controller.get_apvs(pvnames)
        self.assertEqual( len(all_apvs), 3)
        for pvname in pvnames:
            self.assertIn(pvname, all_apvs)
 
        singleapv = controller.get_apv('DEFAULT.PV')
        defaultapv = all_apvs['DEFAULT.PV']
        
        self.assertEqual( singleapv, defaultapv)
        self.assertEqual( defaultapv.name, 'DEFAULT.PV' )
        self.assertEqual( defaultapv.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( defaultapv.mode.delta, 0.01 )

        fooapv = all_apvs['FOO.PV']
        self.assertEqual( fooapv.name, 'FOO.PV' )
        self.assertEqual( fooapv.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( fooapv.mode.delta, 0.123 )

        barapv = all_apvs['BAR.PV']
        self.assertEqual( barapv.name, 'BAR.PV' )
        self.assertEqual( barapv.mode.name , SubscriptionMode.Scan.name )
        self.assertEqual( barapv.mode.period, 123.1 )

        self.assertFalse(controller.get_apv('whatever'))
        
        self.assertRaises(TypeError, controller.get_apvs, 'not a collection')

    def test_save_config(self):
        controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01))
        controller.subscribe('FOO.PV',  SubscriptionMode.Monitor(0.123))
        controller.subscribe('BAR.PV', SubscriptionMode.Scan(123.1)) 

        cfg = controller.save_config()
        logger.info("Saved config looks like:\n%s", cfg)

    def test_load_config(self):
        pvnames = ('DEFAULT.PV', 'FOO.PV', 'BAR.PV')
        
        futures = []
        futures.append(controller.subscribe('DEFAULT.PV', SubscriptionMode.Monitor(delta=0.01)))
        futures.append(controller.subscribe('FOO.PV',  SubscriptionMode.Monitor(0.123)))
        futures.append(controller.subscribe('BAR.PV', SubscriptionMode.Scan(123.1)))

        [ fut.wait() for fut in futures ]

        cfg = controller.save_config()

        futures = controller.munsubscribe([pvname for pvname, _ in controller.list_subscribed()])
        [ fut.wait() for fut in futures ]

        logger.info("Trying to load:\n%s", cfg)

        futures = controller.load_config(cfg)
        results = [ fut.get() for fut in futures ]

        pvnames = [pvname for pvname, _ in controller.list_subscribed()]

        self.assertIn('DEFAULT.PV', pvnames)
        self.assertIn('FOO.PV', pvnames)
        self.assertIn('BAR.PV', pvnames)

        av = controller.get_apv('DEFAULT.PV')
        self.assertEqual( av.name, 'DEFAULT.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( av.mode.delta, 0.01 )

        av = controller.get_apv('FOO.PV')
        self.assertEqual( av.name, 'FOO.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Monitor.name )
        self.assertEqual( av.mode.delta, 0.123 )

        av = controller.get_apv('BAR.PV')
        self.assertEqual( av.name, 'BAR.PV' )
        self.assertEqual( av.mode.name, SubscriptionMode.Scan.name )
        self.assertEqual( av.mode.period, 123.1 )

    def tearDown(self):
        controller.shutdown()
        #datastore.reset_schema(settings.DATASTORE['servers'][0], settings.DATASTORE['keyspace'])

    @classmethod
    def tearDownClass(cls):
        _block_ioc(False)






