from handlers.base import BaseHandler
import json 
from tornado.ioloop import IOLoop

from caa import controller, SubscriptionMode

import logging
import datetime 
import fnmatch

logger = logging.getLogger('server.caa.' + __name__)

controller.initialize()

class SubscriptionHandler(BaseHandler):
    def get(self):
        pvpattern = self.get_argument('pvname', '*')
        mpattern = self.get_argument('mode', '*')
        
        apvs_gen = controller.get_pvs(pvpattern, mpattern)
        res = [ {'pvname': apv.name, 'mode': apv.mode} for apv in apvs_gen ]
        self.win(res)
        
    def post(self):
        body_json = self.request.body
        if not body_json:
            self.fail("Empty request")
        try:
            body = json.loads(body_json)
            pvname = body['pvname']
            mode_raw = body['mode']
            mode = SubscriptionMode.parse(mode_raw)
            controller.subscribe(pvname, mode)
            self.win({'pvname': pvname, 'mode': mode})
        except Exception as e:
            logger.exception(e)
            self.fail(str(e))

    def delete(self):
        body_json = self.request.body
        if not body_json:
            self.fail("Empty request")
        try:
            body = json.loads(body_json)
            pvname = body['pvname']
            controller.unsubscribe(pvname)
            self.win({'pvname': pvname})
        except Exception as e:
            logger.exception(e)
            self.fail(str(e))

class InfoHandler(BaseHandler):
    def get(self):
        pvname = self.get_argument('pvname')
        apv = controller.get_info(pvname)
        self.win(apv)

class StatusHandler(BaseHandler):
    def get(self): 
        pvname = self.get_argument('pvname')

        status = controller.get_status(pvname)
        self.win(status if status else None)

class ValuesHandler(BaseHandler):
    def get(self):
        pvname = self.get_argument('pvname')
        fields = self.get_arguments('field')
        limit = int(self.get_argument('limit', default=100))
        from_date = self.get_argument('from_date', default=None)
        to_date = self.get_argument('to_date', default=None)

        # transform from_date and to_date from epoch secs to datetime
        if from_date:
            from_date = datetime.datetime.fromtimestamp(float(from_date))
        if to_date:
            to_date = datetime.datetime.fromtimestamp(float(to_date))
        
        try:
            res = controller.get_values(pvname, fields, limit, from_date, to_date)
            self.win(res if res else None)
        except KeyError as e:
            self.fail("Invalid field: %s" % e)
        except Exception as e:
            self.fail("Error: %s" % e)


import cStringIO
from contextlib import closing
class ConfigHandler(BaseHandler):
    def get(self):
        with closing(cStringIO.StringIO()) as f:
            controller.save_config(f)
            f.seek(0)
            self.write(f.read())

    def post(self):
        data = self.request.body
        with closing(cStringIO.StringIO(data)) as fobj:
            try:
                import pdb
                pdb.set_trace()
                res = [ str(uuid) for uuid in controller.load_config(fobj) ]
                self.win(res)
            except Exception as e:
                self.fail(e)


