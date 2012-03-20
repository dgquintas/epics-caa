#!/usr/bin/env python
import unittest

TEST_MODULES = [
    'controller',
]

def all():
    return unittest.defaultTestLoader.loadTestsFromNames(TEST_MODULES)

if __name__ == '__main__':
    import tornado.testing
    import os
    from caa.conf import ENVIRONMENT_VARIABLE
    os.environ[ENVIRONMENT_VARIABLE] = 'caa.settings_dev'

    tornado.testing.main()
