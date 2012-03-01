import unittest
import os

from caa.conf import settings, ENVIRONMENT_VARIABLE


class TestConfig(unittest.TestCase):
 
    def setUp(self):
        os.environ[ENVIRONMENT_VARIABLE] = 'settings_test'

    def tearDown(self):
        settings.reset()

    def test_values(self):
        directive = settings.DIRECTIVE
        keys = ('key_to_string', 'key_to_list', 'key_to_dict', 'key_to_int')
        for k in keys:
            self.assertIn(k, directive.keys())

        s = directive['key_to_string']
        self.assertEqual(s, 'string_value')

        l = directive['key_to_list']
        self.assertIs(type(l), list)
        self.assertTrue(len(l), 2)
        self.assertIn('listitem1', l) 
        self.assertIn('listitem2', l) 

        d = directive['key_to_dict']
        self.assertEqual(d['foo'], 1)
        self.assertEqual(d['bar'], 2)

        i = directive['key_to_int']
        self.assertEqual(i, 42)

        another = settings.ANOTHER_DIR
        self.assertEqual( another, "still kickin'")


    def test_no_envvar(self):
        del os.environ[ENVIRONMENT_VARIABLE]
        self.assertRaises(ImportError, getattr, settings, "DATASTORE")
        
    def test_diff_settings_file(self):
        os.environ[ENVIRONMENT_VARIABLE] = 'settings_test_2'
        foo = settings.FOO
        self.assertEqual(foo, 'bar')


