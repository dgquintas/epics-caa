from handlers.base import BaseHandler
from tornado.ioloop import IOLoop

from caa import controller, SubscriptionMode

import logging
import datetime 

logger = logging.getLogger('server.caa.' + __name__)

class SubscriptionHandler(BaseHandler):
    def get(self):
        # we expect the mode and its parameters to
        # be encoded in the request's arguments
        
        pvname = self.get_argument('pvname')
        mode_name = self.get_argument('mode')

        available_modes_names = [ m.name for m in SubscriptionMode.available_modes]
        if mode_name not in available_modes_names:
            self.fail("Unknown mode '%s'" % mode_name)
        else:
            # we leave it up to SubscriptionMode.parse to
            # determine if the arguments to the mode have
            # been been actually passed 
            args = dict(self.request.arguments)
            args.pop('pvname')
            mode_args = dict( ( (k,v[0]) for k,v in args.iteritems() ) )
            try:
                mode = SubscriptionMode.parse(mode_args)
            except Exception as e:
                self.fail(e)
                raise

            # create subscription to pvname with mode
            controller.subscribe(pvname, mode)

            self.win()
            

class UnsubscriptionHandler(BaseHandler):
    def get(self): 
        pvname = self.get_argument('pvname')
        res = controller.unsubscribe(pvname)
        self.win(res)

class InfoHandler(BaseHandler):
    def get(self):
        pvname = self.get_argument('pvname')

        info = controller.get_info(pvname)
        self.win(info)

class StatusHandler(BaseHandler):
    def get(self): 
        pvnames = self.get_arguments('pvname')

        status = controller.get_status(pvnames)
        self.win(status)

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
            
        res = controller.get_values(pvname, fields, limit, from_date, to_date)
        self.win(res)

import cStringIO
class ConfigHandlerSave(BaseHandler):
    def get(self):
        f = cStringIO.StringIO()
        controller.save_config(f)
        f.seek(0)
        self.write(f.read())

class ConfigHandlerLoad(BaseHandler):
    def put(self):
        data = self.request.body
        try:
            res = controller.load_config(data)
            self.win(res)
        except Exception as e:
            self.fail(e)

class ShutdownHandler(BaseHandler):
    def get(self):
        controller.shutdown()
        self.win(None)
        ioloop = IOLoop.instance()
        ioloop.stop()

