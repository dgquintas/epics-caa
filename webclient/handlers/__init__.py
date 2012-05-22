import json
import time
import operator
import urllib
import urlparse 
import tornado.web
import tornado.httpclient
from tornado.options import options
from conf import settings

from datetime import datetime

import logging
logger = logging.getLogger('boilerplate.' + __name__)

baseurl = options.caa_server
logger.info("Using '%s' as the archiver's URL", baseurl)

config = settings.WEBCLIENT

def _caa_httpclient():
    http = tornado.httpclient.HTTPClient()
    def fetch(path, **kwargs):
        url = urlparse.urljoin(baseurl, path)
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
        response = caa_fetch('/archives/') 
        body = json.loads(response.body)

        all_apvs = sorted(body['response'].values(), key=operator.itemgetter('name'))
        subscribed_apvs = sorted((apv for apv in all_apvs if apv['subscribed']), key=operator.itemgetter('name'))

        response = caa_fetch('/statuses/')
        body = json.loads(response.body)
        statuses = body['response']
        # statuses is a list of lists. In this case, all the sublists consist
        # of a single element (the last status). There'll be one sublist per
        # subscribed PV
        ss = dict((status[0]['pvname'], status[0]) for status in statuses if len(status) > 0) 
        self.render("frontpage.html", subscribed_apvs=subscribed_apvs, all_apvs=all_apvs, 
                statuses=ss)

class PVHandler(BaseHandler):
    def delete(self, pvname):
        response = caa_fetch('/subscriptions/', method='DELETE')
        self.write(json.loads(response.body))

    @tornado.web.addslash
    def get(self, pvname):
        response = caa_fetch('/archives/' + pvname) 
        body = json.loads(response.body)
        if not body['status']['success']:
            raise tornado.web.HTTPError( body['status']['code'], body['status']['message'] )

        apv = body['response']
        
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

        nextpage = self.get_argument('nextpage', None)
        prevpage = self.get_argument('prevpage', None)

        args_list = [ ('field', f) for f in fields ]
        args_list += [ ('field', 'archived_at') ]
        if limit:
            args_list += [('limit', limit)]
        if nextpage:
            args_list += [('nextpage', nextpage)]
        if prevpage:
            args_list += [('prevpage', prevpage)]
            
        if fromdate and fromtime:
            # translate to epoch timestamp
            datetimestr = ' '.join((fromdate, fromtime))
            dt = datetime.strptime(datetimestr, '%Y/%m/%d HH:MM')
            ts = time.mktime(dt.timetuple()) * 1e6
            args_list += [('from_ts', ts)]
        if todate and totime:
            # translate to epoch timestamp
            datetimestr = ' '.join((todate, totime))
            dt = datetime.strptime(datetimestr, '%Y/%m/%d HH:MM')
            ts = time.mktime(dt.timetuple()) * 1e6
            args_list += [('to_ts', ts)]

        qargs = urllib.urlencode(args_list)
        response = caa_fetch('/values/' + pvname + '?' + qargs);
        body = json.loads(response.body)
        values_dict = body['response']
        values = values_dict['rows']

        current_url = self.request.full_url()
        url_parts = urlparse.urlparse(current_url)
        qargs = urlparse.parse_qsl(url_parts.query)
        qargs[:] = [qarg for qarg in qargs if \
                (qarg[0] not in ('nextpage', 'prevpage'))]

        pointer_urls = {}
        for pointer in ('nextpage', 'prevpage'):
            url = values_dict.get(pointer)
            if url:
                # extract qarg
                qarg = urlparse.parse_qs(url)[pointer][0]
                # replace it in current url
                qargs.append((pointer, qarg))
                qs = urllib.urlencode(qargs)
                pointer_urls[pointer] = \
                        urlparse.urlunparse(url_parts._replace(query=qs))

        self.render('pv.html', apv=apv, statuses=statuses, 
        available_fields=available_fields, fields=fields, limit=limit, 
        fromdate=fromdate, todate=todate, fromtime=fromtime, totime=totime, 
        values=values, nextpage=pointer_urls['nextpage'], 
        prevpage=pointer_urls['prevpage'])

class SubscriptionHandler(BaseHandler):
    def delete(self):
        response = caa_fetch('/subscriptions/', method='DELETE')
        self.write(json.loads(response.body))
        #if self.check_response(response):
        #    self.render('successful_subscription.html', pvname=pvname, mode=mode)
        #else:
        #    self.render('unsuccessful_subscription.html', pvname=pvname, 
        #            request=request_json, response=response)


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
                        'delta': float(delta), 
                        'max_freq': float(max_freq)}
            elif modename == 'Scan':
                period = self.get_argument('period')
                mode = {'mode': modename,
                        'period': float(period)}
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



class ConfigHandler(BaseHandler):
    @tornado.web.addslash
    def get(self):
        response = caa_fetch('/config')
        body = response.body
        self.set_header('Content-Type', 'text/plain')
        self.write(body)

    def post(self):
        # get the file submitted to us first
        config = self.request.files.get('configfile')
        config_contents = config[0]['body']
        if config and len(config):
            response = caa_fetch('/config', method='PUT', body=config_contents)
        if self.check_response(response):
            self.render('successful_config_load.html')
        else:
            self.render('unsuccessful_config_load.html', response=response, config=config_contents)




