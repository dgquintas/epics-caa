import unittest

import caa

import pycassa
import pycassa.system_manager as SM
from pycassa.types import *

class TestDataStore(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.config = caa.Config('config_testing.cfg')
        caa.servicelocator.set_feature('config', cls.config)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_create_schema(self):
        caa.create_schema(self.config.datastore['servers'][0], 
                self.config.datastore['keyspace'],
                recreate=True)
    
    @classmethod
    def tearDownClass(cls):
        pass





