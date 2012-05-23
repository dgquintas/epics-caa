from handlers.base import BaseHandler
import json 
import urlparse
import urllib 
import tornado.web

from caa import controller, SubscriptionMode

import logging

logger = logging.getLogger('server.caa.' + __name__)

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
            results = dict( (pvname, f.get(TIMEOUT)) for pvname, f in zip(pvnames, futures) ) 
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

class PVValuesHandler(BaseHandler):
    def get(self, pvname):
        fields = self.get_arguments('field')
        limit = int(self.get_argument('limit', default=10))
        from_ts = self.get_argument('from_ts', default=None)
        to_ts = self.get_argument('to_ts', default=None)


        nextpage = self.get_argument('nextpage', None)
        prevpage = self.get_argument('prevpage', None)
        
        reverse = True
        if nextpage:
            from_ts = nextpage
        if prevpage:
            from_ts = prevpage
            reverse = False

        next_url = None
        prev_url = None
        at_first_page = False
 
        rows = controller.get_values(pvname, fields, limit+1, from_ts, to_ts, reverse)
        if prevpage: # we come from a prevpage link
            # if a prevpage has been requested while already at the first page, 
            # a single row is returned, that with the prevpage's pointer value
            # in its archived_at_ts field
            at_first_page = len(rows) == 1
            if not at_first_page:
                rows.reverse()

                

       
        if rows:
            # check if we are at the end
            if len(rows) > limit or at_first_page: #not at the end
                last = rows.pop()
                next_ts = last['archived_at_ts']
            else: # at the end
                next_ts = None

             
            current_url = self.request.full_url()
            url_parts = urlparse.urlparse(current_url)
            qargs = urlparse.parse_qsl(url_parts.query)
            qargs[:] = [qarg for qarg in qargs if \
                    (qarg[0] not in ('nextpage', 'prevpage'))]
            
            if next_ts:
                qargs.append(('nextpage', next_ts))
                qs = urllib.urlencode(qargs)
                next_url = urlparse.urlunparse(url_parts._replace(query=qs))
                qargs.pop() # restore, get ready for prevpage
            
            if not at_first_page:
                curr_hdr_st = rows[0]['archived_at_ts']

                qargs.append(('prevpage', curr_hdr_st))
                qs = urllib.urlencode(qargs)
                prev_url = urlparse.urlunparse(url_parts._replace(query=qs))

        self.win({'rows': rows, 
                  'nextpage': next_url,
                  'prevpage': prev_url})

##########################################################################

class ConfigHandler(BaseHandler):
    def get(self):
        cfg = controller.save_config()
        self.set_header('Content-Type', 'application/json')
        self.write(cfg)

    def put(self):
        data = self.request.body
        try:
            futures = controller.load_config(data)
            results = dict( (f.taskname, f.get(TIMEOUT)) for f in futures ) 
            self.win(results)
        except Exception as e:
            self.fail(e)

class SettingsHandler(BaseHandler):
    def get(self, section):
        settings = controller.get_settings()
        section_settings = getattr(settings, section, None)
        self.win(section_settings)

