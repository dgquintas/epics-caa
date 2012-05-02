from handlers.base import BaseHandler
import json 
import tornado.web

from caa import controller, SubscriptionMode

import logging
import datetime 

logger = logging.getLogger('server.caa.' + __name__)

# XXX: this is kinda dirty. Maybe it should be handled independently of this server
controller.initialize()

TIMEOUT = 1

class RootArchivesHandler(BaseHandler):
    @tornado.web.addslash
    def get(self):
        self.win(dict(controller.list_archived()))

class PVArchivesHandler(BaseHandler):
    def get(self, pvname):
        apv = controller.get_apv(pvname)
        if apv:
            self.win(apv)
        else:
            self.fail(msg="Unknown PV '%s'" % pvname, code=404)

class RootSubscriptionHandler(BaseHandler):
    @tornado.web.addslash
    def get(self):
        modename = self.get_argument('mode', None)
        if modename:
            apvs = dict(controller.list_by_mode(modename))
        else:
            apvs = dict(controller.list_subscribed())
        self.win(apvs)

    def post(self):
        self._subscribe()

    def put(self):
        self.delete()
        self._subscribe()

    def delete(self):
        # unsubscribe from all
        pvs = [pvname for pvname, _ in controller.list_subscribed()]
        futures = controller.munsubscribe(pvs)
        results = dict( (pvname, f.get(TIMEOUT)) for pvname, f in zip(pvs, futures) )
        self.win(results)

    def _subscribe(self):
        body_json = self.request.body
        if not body_json:
            self.fail("Empty request")
        try:
            body = json.loads(body_json)
            # body should of the form
            # [ {pvname: ..., mode: ...}, {pvname: ..., mode: ...}, ... ]
            pvnames = []
            modes = []
            for subscr_dict in body:
                pvnames.append(subscr_dict['pvname'])
                modes.append(SubscriptionMode.parse(subscr_dict['mode']))

            futures = controller.msubscribe(pvnames, modes)
            results = dict( (pvname, f.get(TIMEOUT) for pvname, f in zip(pvnames, futures)) )
            self.win(results)
        except Exception as e:
            logger.exception(e)
            self.fail(str(e))


class PVSubscriptionHandler(BaseHandler):
    def get(self, pvname):
        apv = controller.get_apv(pvname)
        if apv:
            self.win(apv)
        else:
            self.fail(msg="Unknown PV '%s'" % pvname, code=404)

    def put(self, pvname):
        # modify if exists, ie, unsubscribe and resubscribe
        apv = controller.get_apv(pvname)
        if apv and apv.subscribed:
            controller.unsubscribe(pvname)

        # at this point, subscribe no matter what: if we were
        # already subscribed, we've just unsubscribed.

        body_json = self.request.body
        body = json.loads(body_json)
        # body should of the form
        # {mode: ...}
        mode = SubscriptionMode.parse(body['mode'])
        try:
            future = controller.subscribe(pvname, mode)
            result = future.get(TIMEOUT)
            if result:
                self.win({'pvname': pvname, 'mode': mode})
            else:
                self.fail("Couldn't subscribe", {'pvname': pvname, 'mode': mode})
        except Exception as e:
            logger.exception(e)
            self.fail(str(e))

    def delete(self, pvname):
        response = {'pvname': pvname}
        try:
            future = controller.unsubscribe(pvname)
            if future:
                res = future.get(TIMEOUT)
                if res:
                    self.win(response)
                else:
                    self.fail(res, code=408, response=response)
            else: # no future
                self.fail("Unknown PV '%s'" % pvname, code=404)
        except Exception as e:
            logger.exception(e)
            self.fail(str(e))

##########################################################################

class RootStatusesHandler(BaseHandler):
    @tornado.web.addslash
    def get(self):
        limit = self.get_argument('limit', 1)
        pvnames = [pvname for pvname, _ in controller.list_subscribed()]
        statuses = [ controller.get_statuses(pvname, limit) for pvname in pvnames ]
        self.win(statuses)

class PVStatusesHandler(BaseHandler):
    def get(self, pvname):
        limit = self.get_argument('limit', 10)
        statuses = controller.get_statuses(pvname, limit)
        if statuses:
            self.win(statuses)
        else:
            self.fail(msg="Unknown PV '%s'" % pvname, code=404)


##########################################################################

#class RootValuesHandler(BaseHandler):
#    @tornado.web.addslash
#    def get(self):
#        fields = self.get_arguments('field')
#        limit = int(self.get_argument('limit', default=1))
#        from_date = self.get_argument('from_date', default=None)
#        to_date = self.get_argument('to_date', default=None)
#
#        # transform from_date and to_date from epoch secs to datetime
#        if from_date:
#            from_date = datetime.datetime.fromtimestamp(float(from_date))
#        if to_date:
#            to_date = datetime.datetime.fromtimestamp(float(to_date))
#        
#        values = controller.get_values(pvname, fields, limit, from_date, to_date) 
#
#        self.win(values)

class PVValuesHandler(BaseHandler):
    def get(self, pvname):
        fields = self.get_arguments('field')
        limit = int(self.get_argument('limit', default=10))
        from_date = self.get_argument('from_date', default=None)
        to_date = self.get_argument('to_date', default=None)

        # transform from_date and to_date from epoch secs to datetime
        if from_date:
            from_date = datetime.datetime.fromtimestamp(float(from_date))
        if to_date:
            to_date = datetime.datetime.fromtimestamp(float(to_date))
        res = controller.get_values(pvname, fields, limit, from_date, to_date)
        self.win(res)

##########################################################################


class ConfigHandler(BaseHandler):
    def get(self):
        cfg = controller.save_config()
        self.set_header('Content-Type', 'application/json')
        self.write(cfg)

    def put(self):
        data = self.request.body
        try:
            res = [ str(uuid) for uuid in controller.load_config(data) ]
            self.win(res)
        except Exception as e:
            self.fail(e)

class SettingsHandler(BaseHandler):
    def get(self, section):
        settings = controller.get_settings()
        section_settings = getattr(settings, section, None)
        self.win(section_settings)

