import json
import urllib
from urlparse import urljoin
import tornado.web
import tornado.httpclient
from tornado.options import options
from conf import settings

from datetime import datetime

import logging
logger = logging.getLogger('boilerplate.' + __name__)

baseurl = options.caa_server

config = settings.WEBCLIENT

def _caa_httpclient():
    http = tornado.httpclient.HTTPClient()
    def fetch(path, **kwargs):
        url = urljoin(baseurl, path)
        return http.fetch(url, **kwargs)
    return fetch

caa_fetch = _caa_httpclient()

class BaseHandler(tornado.web.RequestHandler):
    def check_response(self, response):
        # response has the following format (server.handlers.base:envelope)
        # {"status": { "success": success,
        #              "code": code,
        #              "codestr": ...
        #              "message": msg,
        #            },
        # "response": response,}
        assert hasattr(response, 'body')
        d = json.loads(response.body)
        if 'status' in d:
            return d['status']['success']
        else:
            raise ValueError("Invalid response body: '%s'" % d)

    def get_status_msg(self, response):
        assert hasattr(response, 'body') 
        d = json.loads(response.body)
        if 'status' in d:
            return d['status']['message']
        else:
            raise ValueError("Invalid response body: '%s'" % d)

    def get_response(self, response):
        assert hasattr(response, 'body')
        d = json.loads(response.body)
        return d['response']

class RootHandler(BaseHandler):
    def get(self):
        response = caa_fetch('/subscriptions/') 
        body = json.loads(response.body)
        pvs_dict = body['response']

        response = caa_fetch('/statuses/')
        body = json.loads(response.body)
        statuses = body['response']
        # statuses is a list of lists. In this case, all the sublists consist
        # of a single element (the last status). There'll be one sublist per
        # subscribed PV
        statuses = dict((status[0]['pvname'], status[0]) for status in statuses) 
        self.render("frontpage.html", apvs=pvs_dict.values(), statuses=statuses)

class PVHandler(BaseHandler):
    @tornado.web.addslash
    def get(self, pvname):
        response = caa_fetch('/subscriptions/' + pvname) 
        body = json.loads(response.body)
        if not body['status']['success']:
            raise tornado.web.HTTPError( body['status']['code'], body['status']['message'] )

        response = body['response']
        since = datetime.fromtimestamp( response['since'] / 1e6 )
        name = response['name']
        mode = response['mode']

        response = caa_fetch('/statuses/' + pvname) 
        body = json.loads(response.body)
        statuses = body['response']
  
        response = caa_fetch('/settings/ARCHIVER')
        body = json.loads(response.body)
        available_fields = body['response']['pvfields']

        fields = self.get_arguments('field') or config['default_table_fields']
        limit = self.get_argument('limit', 10)
        fromdate = self.get_argument('from_date', None)
        todate = self.get_argument('to_date', None)
        fromtime = self.get_argument('from_time', None)
        totime = self.get_argument('to_time', None)
        
        args_list = [ ('field', f) for f in fields ]
        if limit:
            args_list += [('limit', limit)]
        if fromdate and fromtime:
            # translate to epoch timestamp
            datetimestr = ' '.join((fromdate, fromtime))
            dt = datetime.strptime(datetimestr, '%Y/%m/%d HH:MM')
            args_list += [('from_date', time.mktime(dt.timetuple()))]
        if todate and totime:
            # translate to epoch timestamp
            datetimestr = ' '.join((todate, totime))
            dt = datetime.strptime(datetimestr, '%Y/%m/%d HH:MM')
            args_list += [('to_date', time.mktime(dt.timetuple()))]

        qargs = urllib.urlencode(args_list)
        response = caa_fetch('/values/' + pvname + '?' + qargs);
        body = json.loads(response.body)
        values = body['response']
 
        self.render('pv.html', name=name, mode=mode, since=since, statuses=statuses, available_fields=available_fields, 
                fields=fields, limit=limit, fromdate=fromdate, todate=todate, fromtime=fromtime, totime=totime,
                values=values)
 

class SubscriptionHandler(BaseHandler):
    def post(self):
        # body must contain 'pvname' and 'mode'
        body_json = self.request.body
        if not body_json:
            self.fail("Empty request")
        try:
            pvname = self.get_argument('pvname')
            modename = self.get_argument('modename')
            if modename == 'Monitor':
                delta = self.get_argument('delta')
                max_freq = self.get_argument('max_freq', 0.0)
                mode = {'mode': modename, 
                        'delta': delta, 
                        'max_freq': max_freq}
            elif modename == 'Scan':
                period = self.get_argument('period')
                mode = {'mode': modename,
                        'period': period}
            else:
                raise ValueError("Unknown mode name '%s'" % modename)

            request = [{'pvname': pvname, 'mode': mode}]
            request_json = json.dumps(request)
            response = caa_fetch('/subscriptions/', method='POST', body=request_json)
            if self.check_response(response):
                self.render('successful_subscription.html', pvname=pvname, mode=mode)
            else:
                self.render('unsuccessful_subscription.html', pvname=pvname, 
                        request=request_json, response=response)
        except:
            logger.exception("Error while processing subscription")
            raise
