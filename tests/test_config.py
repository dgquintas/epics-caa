import unittest
from caa import config

class TestConfig(unittest.TestCase):
    
    def setUp(self):
        self.cfg = config.get_config('config.test.cfg')

    def test_parsing_error(self):
        import StringIO
        from contextlib import closing
        with closing(StringIO.StringIO("{this ain't valid yo")) as f:
            f.name = "FakeFile"
            self.assertRaises(ValueError, config._Config, f)

    def test_sections(self):
        sections = self.cfg.sections
        for section in sections:
            sect = getattr(self.cfg, section)
            self.assertTrue(sect)
        self.assertRaises(AttributeError, getattr, self.cfg, 'FOOBAR')

    def test_values(self):
        datastore = self.cfg.DATASTORE
        keys = ('keyspace', 'servers', 'replication_factor')
        for k in keys:
            self.assertIn(k, datastore.keys())

        self.assertEqual(datastore['keyspace'], 'caaTest')

        servers = datastore['servers']
        self.assertIs(type(servers), list)
        self.assertTrue(len(servers), 2)
        self.assertIn('localhost:9160', servers) 
        self.assertIn('localhost:9161', servers) 

        controller = self.cfg.CONTROLLER
        self.assertIs(type(controller['epics_connection_timeout']), float)
        self.assertIs(type(controller['num_timers']), int)




