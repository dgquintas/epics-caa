import unittest
from caa import datastore

import pdb

import pycassa
import pycassa.system_manager as SM
from pycassa.types import *

from testing_settings import *

class TestDataStore(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.sm = SM.SystemManager()
        cls.sm.create_keyspace(KEYSPACE, replication_factor=1)

        cls.pool = pycassa.pool.ConnectionPool(KEYSPACE)

    def setUp(self):
        self.sm.create_column_family(KEYSPACE, CF)
        #self.sm.alter_column(KEYSPACE, CF, 'one', self.sm.INT_TYPE)
        self.data_store = datastore.DataStore(KEYSPACE, CF)

    def test_read_write(self):
        key1 = 'foo'
        data1 = {'one': '1', 'two': '2'}
        self.data_store.write(key1, data1)

        key2 = 'bar'
        data2 = {'pi': '3.141592', 'e': '2.718281'}
        self.data_store.write(key2, data2)

        readback_dict = self.data_store.read([key1])
        self.assertIn( key1 ,readback_dict )
        self.assertIn( 'one', readback_dict[key1] )
        self.assertIn( 'two', readback_dict[key1] )
        self.assertEqual( '1', readback_dict[key1]['one'] )
        self.assertEqual( '2', readback_dict[key1]['two'] )


        readback_dict = self.data_store.read([key2])
        self.assertIn( key2,  readback_dict ) 
        self.assertIn( 'pi',  readback_dict[key2] )
        self.assertIn( 'e' , readback_dict[key2] ) 
        self.assertEqual( '3.141592', readback_dict[key2]['pi'] )
        self.assertEqual( '2.718281', readback_dict[key2]['e'] )

    def test_multiread(self):
        key1 = 'foo'
        data1 = {'one': '1', 'two': '2'}
        self.data_store.write(key1, data1)

        key2 = 'bar'
        data2 = {'pi': '3.141592', 'e': '2.718281'}
        self.data_store.write(key2, data2)

        readback_dict = self.data_store.read([key1, key2])
        self.assertIn( key1, readback_dict )
        self.assertIn( 'one', readback_dict[key1] )
        self.assertIn( 'two', readback_dict[key1] )
        self.assertEqual( '1', readback_dict[key1]['one'] )
        self.assertEqual( '2', readback_dict[key1]['two'] )


        self.assertIn( key2, readback_dict ) 
        self.assertIn( 'pi', readback_dict[key2] )
        self.assertIn( 'e', readback_dict[key2] ) 
        self.assertEqual( '3.141592', readback_dict[key2]['pi'] )
        self.assertEqual( '2.718281', readback_dict[key2]['e'] )

    def test_read_nonexistent(self):
        key1 = 'foo'
        data1 = {'one': '1', 'two': '2'}
        self.data_store.write(key1, data1)

        res = self.data_store.read(['foo', 'bar'])

        self.assertIn( 'foo', res )
        self.assertTrue( res['foo'] )
        self.assertIn( 'bar', res )
        self.assertTrue( res['bar'] == {} ) 

    def tearDown(self):
        self.sm.drop_column_family(KEYSPACE,CF)

    @classmethod
    def tearDownClass(cls):
        cls.sm.drop_keyspace(KEYSPACE)
        cls.pool.dispose()





