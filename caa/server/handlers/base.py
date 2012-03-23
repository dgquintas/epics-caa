import json
from httplib import responses
import traceback

import tornado.web
from tornado.escape import json_encode

import logging
logger = logging.getLogger('boilerplate.' + __name__)

def envelope(success, code, msg, response):
    d = {"status": {  "success": success,
                      "code": code,
                      "codestr": responses.get(code, 'Unknown. FIXME!'),
                      "message": msg,
                   },
         "response": response,}
    return d
            
class BaseHandler(tornado.web.RequestHandler):
    """A class to collect common handler methods - all other handlers should
    subclass this one.
    """

    def fail(self, msg, code=400, response=None):
        d = envelope(False, code, msg, response)
        self.write(d)

    def win(self, results):
        d = envelope(True, 200, 'ok', results)
        self.write(d)
     
    def write_error(self, code, **kwargs):
        trace = kwargs.get('exc_info')
        if trace:
            tb = traceback.format_exception(*trace)
            tbstr = ''.join(tb) 
            exception = traceback.format_exception_only(trace[0], trace[1])
            response = tbstr
            msg = msg
        else:
            response = None
            msg = "Unspecified error ocurred"

        self.fail(code=code, response=response, msg=msg)
