import unittest
from caa import SubscriptionMode

class TestSubscriptionMode(unittest.TestCase):
    
    def setUp(self):
        pass

    def test_parse(self):
        wrong_mode = {'mode': 'foo'}
        wrong_everything = {'foo': 'bar'}

        self.assertRaises(ValueError, SubscriptionMode.parse, wrong_mode)
        self.assertRaises(KeyError, SubscriptionMode.parse, wrong_everything)


    def test_parse_monitor(self):
        monitor_ok = {'mode': 'Monitor', 'delta': 1.23, 'max_freq': 7, "it's alright": "to have extra fields"}
        monitor_nodelta= {'mode': 'Monitor', 'max_freq': 7}
        monitor_nofreq= {'mode': 'Monitor', 'delta': 7.1}
        monitor_empty= {'mode': 'Monitor'}
        
        d = monitor_ok
        m = SubscriptionMode.parse(d)
        self.assertEqual(m.name, d['mode'])
        self.assertEqual(m.delta, d['delta'])
        self.assertEqual(m.max_freq, d['max_freq'])

        d = monitor_nodelta
        m = SubscriptionMode.parse(d)
        self.assertEqual(m.name, d['mode'])
        self.assertEqual(m.delta, 0.0) # delta defaults to 0.0
        self.assertEqual(m.max_freq, d['max_freq'])
        
        d = monitor_nofreq
        m = SubscriptionMode.parse(d)
        self.assertEqual(m.name, d['mode'])
        self.assertEqual(m.delta, d['delta']) 
        self.assertEqual(m.max_freq, None) # max_freq defaults to None

        d = monitor_empty
        m = SubscriptionMode.parse(d)
        self.assertEqual(m.name, d['mode'])
        self.assertEqual(m.delta, 0.0) 
        self.assertEqual(m.max_freq, None)
 
    def test_parse_scan(self):
        scan_ok = {'mode': 'Scan', 'period': 3.14}
        scan_empty= {'mode': 'Scan', 'this': "ain't used"}
        
        d = scan_ok
        m = SubscriptionMode.parse(d)
        self.assertEqual(m.name, d['mode'])
        self.assertEqual(m.period, d['period'])

        d = scan_empty
        self.assertRaises(ValueError, SubscriptionMode.parse, d)

    def test_eq(self):
        m1 = {'mode': 'Monitor', 'delta': 1.23, 'max_freq': 7}
        m11 = {'mode': 'Monitor', 'delta': 1.23, 'max_freq': 7}

        m2 = {'mode': 'Monitor', 'delta': 2.23, 'max_freq': 4}

        self.assertEqual(m1, m11)
        self.assertNotEqual(m1,m2)

        s1 = {'mode': 'Scan', 'period': 1}
        s11 = {'mode': 'Scan', 'period': 1}
        s2 = {'mode': 'Scan', 'period': 2}

        self.assertEqual(s1, s11)
        self.assertNotEqual(s1, s2)

    def test_as_dict(self):
        monitor_ok = {'mode': 'Monitor', 'delta': 1.23, 'max_freq': 7, 
                "it's alright": "to have extra fields"}
        m = SubscriptionMode.parse(monitor_ok)

        m_back = dict(m)
        m2 = SubscriptionMode.parse(m_back)

        self.assertEqual(m, m2)
        

    def tearDown(self):
        pass

