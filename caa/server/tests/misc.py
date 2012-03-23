from tornado.testing import AsyncHTTPTestCase
from caa.server import app

from json import loads as json_decode
from json import dumps as json_encode

from pprint import pprint as pp

class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        AsyncHTTPTestCase.__init__(self, *args, **kwargs)
        self.maxDiff=None

    def get_app(self):
        return app.TornadoApp()

    def setUp(self):
        AsyncHTTPTestCase.setUp(self)

    def tearDown(self):
        AsyncHTTPTestCase.tearDown(self)


class ErrorsTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)

    def test_404(self):
        pass

 
