from tornado.testing import AsyncHTTPTestCase
from caa.server import app
from caa.server.handlers.base import envelope

from caa import controller 
from caa import SubscriptionMode

from json import loads as json_decode
from json import dumps as json_encode
import time
import urllib
import fnmatch

from tornado.httpclient import HTTPRequest


class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        AsyncHTTPTestCase.__init__(self, *args, **kwargs)
        self.maxDiff=None

    def get_app(self):
        return app.TornadoApp()

    def setUp(self):
        AsyncHTTPTestCase.setUp(self)
        reload(controller)
        controller.initialize(recreate=True)

    def tearDown(self):
        AsyncHTTPTestCase.tearDown(self)
        controller.shutdown()

    def _subscribe_to_many(self, pvnames, modedefs):
        to_sub = [{'pvname': pvname, 'mode': modedef} \
                for pvname, modedef in zip(pvnames, modedefs)]
        reqbody = json_encode(to_sub)
        response = self.fetch('/subscriptions/', method='POST', body=reqbody)

        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)


class RootSubscriptionsTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)

    def test_subscriptions_get(self):
        # this first fetch with a trailing slash
        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200, response.error)

        # empty
        expected = envelope(True, 200, 'ok', {})
        decodedbody = json_decode(response.body)
        self.assertEqual(decodedbody, expected)

        # test alphabetical ordering of returned pvs
        pvnames = ("test:long1", "test:double1", "test:double2", "test:long3")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1},
                    {"mode": "Scan", "period": 0.2},
                    {"mode": "Scan", "period": 0.3}, )
        self._subscribe_to_many(pvnames, modedefs)
        # second fetch WITHOUT a trailing slash
        response = self.fetch('/subscriptions')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        responses = json_decode(response.body)['response']
        self.assertEqual(len(responses), len(pvnames))

        self.assertItemsEqual( responses.keys(), pvnames )

        for pvname, modedef in zip(pvnames, modedefs):
            self.assertEqual(responses[pvname]['name'], pvname)
            self.assertEqual(responses[pvname]['mode'], SubscriptionMode.parse(modedef))
        
    def test_subscriptions_post(self):
        pvnames = ("test:long1", "test:long2")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})
        self._subscribe_to_many(pvnames, modedefs)

        # read them back
        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = [ {'pvname': pvname, 'mode': SubscriptionMode.parse(modedef)} \
                for pvname, modedef in zip(pvnames, modedefs) ]
        expected = envelope(True, 200, 'ok', res) 
        decodedbody = json_decode(response.body)
        self.assertEqual(decodedbody['status'], expected['status'])
        
        d = dict( zip(pvnames, modedefs) )
        responses = decodedbody['response']
        self.assertEqual( len(responses), len(pvnames) )
        for pvname, pvdict in responses.iteritems():
            self.assertEqual(pvname, pvdict['name'])
            self.assertIn(pvdict['name'], pvnames)
            m = SubscriptionMode.parse( d[pvdict['name']] )
            self.assertEquals(pvdict['mode'], m)

    def test_subscriptions_put(self):
        pvnames = ("test:long1", "test:long2")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})

        self._subscribe_to_many(pvnames, modedefs)

        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        responses = json_decode(response.body)['response']
        self.assertEqual(len(responses), len(pvnames))
        self.assertItemsEqual( responses.keys(), pvnames )

        for pvname, modedef in zip(pvnames, modedefs):
            self.assertEqual(responses[pvname]['name'], pvname)
            self.assertEqual(responses[pvname]['mode'], SubscriptionMode.parse(modedef))

        # up to here, same as get's test
        
        pvnames = ("test:double1", "test:double2", "test:double3")
        modedefs = ({"mode": "Monitor", "delta": 2.1},
                    {"mode": "Scan", "period": 3.1},
                    {"mode": "Monitor", "period": 4.1},
                    )
        to_sub = [{'pvname': pvname, 'mode': modedef} \
                    for pvname, modedef in zip(pvnames, modedefs)]
        reqbody = json_encode(to_sub)
        response = self.fetch('/subscriptions/', method='PUT', body=reqbody)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        responses = json_decode(response.body)['response']
        self.assertEqual(len(responses), len(pvnames))
        self.assertItemsEqual( responses.keys(), pvnames )

        for pvname, modedef in zip(pvnames, modedefs):
            self.assertEqual(responses[pvname]['name'], pvname)
            self.assertEqual(responses[pvname]['mode'], SubscriptionMode.parse(modedef))

    def test_subscriptions_delete(self):
        pvnames = ("test:long1", "test:long2")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})

        self._subscribe_to_many(pvnames, modedefs)

        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        responses = json_decode(response.body)['response']
        self.assertEqual(len(responses), len(pvnames))
        self.assertItemsEqual( responses.keys(), pvnames )

        for pvname, modedef in zip(pvnames, modedefs):
            self.assertEqual(responses[pvname]['name'], pvname)
            self.assertEqual(responses[pvname]['mode'], SubscriptionMode.parse(modedef))

        # delete now
        response = self.fetch('/subscriptions/', method='DELETE')

        # all subscriptions should be gone now
        response = self.fetch('/subscriptions/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        responses = json_decode(response.body)['response']
        self.assertEqual(len(responses), 0)

class PVSubscriptionsTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)
    
    def test_subscriptions_get(self):
        # non existent
        response = self.fetch('/subscriptions/foobar')
        self.assertEqual(response.code, 200, response.error)
        decodedbody = json_decode(response.body)
        self.assertEqual( decodedbody['status']['code'], 404)
        self.assertFalse( decodedbody['response'] )

        # test alphabetical ordering of returned pvs
        pvnames = ("test:long1", "test:double1", "test:double2", "test:long3")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1},
                    {"mode": "Scan", "period": 0.2},
                    {"mode": "Scan", "period": 0.3}, )
        self._subscribe_to_many(pvnames, modedefs)

        response = self.fetch('/subscriptions/'+pvnames[0])
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = json_decode(response.body)['response']
        self.assertEqual(pvnames[0], res['name'])
        self.assertEqual(res['mode'], SubscriptionMode.parse(modedefs[0]))

    def test_subscriptions_put(self):
        # create subscription, check it. Then modify it and recheck
        pvname = "test:long1"
        mode1 = {"mode": "Monitor", "delta": 1.1}
        mode2 = {"mode": "Scan", "period": 1.1}

        reqbody = json_encode({'mode': mode1})
        response = self.fetch('/subscriptions/'+pvname, method='PUT', body=reqbody)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        response = self.fetch('/subscriptions/'+pvname)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = json_decode(response.body)['response']
        self.assertEqual(pvname, res['name'])
        self.assertEqual(res['mode'], SubscriptionMode.parse(mode1))

        # modify it
        reqbody = json_encode({'mode': mode2})
        response = self.fetch('/subscriptions/'+pvname, method='PUT', body=reqbody)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        response = self.fetch('/subscriptions/'+pvname)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = json_decode(response.body)['response']
        self.assertEqual(pvname, res['name'])
        self.assertEqual(res['mode'], SubscriptionMode.parse(mode2))

    def test_subscriptions_delete(self):
        pvnames = ("test:long1", "test:long2")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})

        self._subscribe_to_many(pvnames, modedefs)
 
        response = self.fetch('/subscriptions/'+pvnames[0])
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = json_decode(response.body)['response']
        self.assertTrue(res)
        self.assertEqual(pvnames[0], res['name'])
        self.assertEqual(res['mode'], SubscriptionMode.parse(modedefs[0]))

        # delete now
        response = self.fetch('/subscriptions/'+pvnames[0], method='DELETE')

        response = self.fetch('/subscriptions/'+pvnames[0])
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        res = json_decode(response.body)['response']
        self.assertFalse(res)

        # delete non existent
        response = self.fetch('/subscriptions/foobar', method='DELETE')
        # the actual request must succeed at the HTTP level...
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        # ... but fail at OUR level
        body = json_decode(response.body)
        self.assertFalse(body['status']['success'])
        self.assertEqual(body['status']['code'], 404)
        self.assertFalse(body['response'])
        

class RootStatusesTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)

    def test_get_statuses(self):
        # without pvname glob
        response = self.fetch('/statuses/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])

        statuses = body['response']
        self.assertEqual(statuses, [])

        # now with some data
        pvnames = ("test:long1", "test:double1")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})
        self._subscribe_to_many(pvnames, modedefs)
        response = self.fetch('/statuses/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])

        ts = time.time() * 1e6
        statuses = body['response']
        self.assertEqual( len(statuses), len(pvnames))
        for status in statuses:
            last = status[-1]
            self.assertTrue(last['connected'])
            self.assertGreaterEqual(ts, last['timestamp'])

    def test_get_statuses_glob(self):
        # with pvname glob
        pvnames = ("test:long1", "test:double1")
        modedefs = ({"mode": "Monitor", "delta": 1.1},
                    {"mode": "Scan", "period": 0.1})
        self._subscribe_to_many(pvnames, modedefs)
        glob = '*long*'
        response = self.fetch('/statuses/?' + urllib.urlencode([('pvname', glob)]))
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])

        ts = time.time() * 1e6
        statuses = body['response']
        self.assertEqual( len(statuses), len(fnmatch.filter(pvnames, glob)) )
        for status in statuses:
            last = status[-1]
            self.assertTrue(last['connected'])
            self.assertGreaterEqual(ts, last['timestamp'])
            self.assertTrue(fnmatch.fnmatch(last['pvname'], glob))


class PVStatusesTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)

    def test_get_statuses(self):
        # without pvname glob
        pass
        #TODO
        

    def test_get_statuses_glob(self):
        # with pvname glob
        pass
        #TODO
 

class RootValuesTest(BaseTest):
    def __init__(self, *args, **kwargs):
        BaseTest.__init__(self, *args, **kwargs)

    def setUp(self):
        BaseTest.setUp(self)

        self.pvnames = ("test:long1", "test:double1")
        self.modedefs = ({"mode": "Monitor", "delta": 0.1},
                    {"mode": "Scan", "period": 1.0})
        self._subscribe_to_many(self.pvnames, self.modedefs)

    def test_get_values(self):
        time.sleep(2)

        response = self.fetch('/values/')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])
        self.assertEqual(body['status']['code'], 200)
        body_response = body['response']
 
        # we should have one value (the last one) for each of the two pvs
        ts = time.time()
        for pvname in self.pvnames:
            self.assertIn(pvname, body_response)
            pvvalues = body_response[pvname]
            self.assertEqual( len(pvvalues), 1)
            last_value = pvvalues[0]
            self.assertAlmostEqual(ts, last_value['timestamp'], delta=2)


    def test_get_values_limit(self):
        time.sleep(4)

        response = self.fetch('/values/?limit=2')
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])
        self.assertEqual(body['status']['code'], 200)
        body_response = body['response']
 
        # we should have two values for each of the two pvs
        ts = time.time()
        for pvname in self.pvnames:
            self.assertIn(pvname, body_response)
            pvvalues = body_response[pvname]
            self.assertEqual( len(pvvalues), 2)
            for pvvalue in pvvalues :
                self.assertAlmostEqual(ts, pvvalue['timestamp'], delta=2)

        #print json_encode(json_decode(response.body), indent=4)


    def test_get_values_glob(self):
        time.sleep(2)

        response = self.fetch('/values/?' + \
                urllib.urlencode([('pvname', '*long*'), ('limit', 2)]))
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])
        self.assertEqual(body['status']['code'], 200)
        body_response = body['response']
 
        # we should have two values for the pv with 'long' in its name
        ts = time.time()
        self.assertEqual(len(body_response), 1)
        self.assertIn('test:long1', body_response)
        pvvalues = body_response['test:long1']
        self.assertEqual( len(pvvalues), 2)
        for pvvalue in pvvalues :
            self.assertAlmostEqual(ts, pvvalue['timestamp'], delta=2)


        #print json_encode(json_decode(response.body), indent=4)

    def test_get_values_fields(self):
        time.sleep(2)
        
        fields = ('pvname', 'value', 'timestamp', 'archived_at')
        queryargs = urllib.urlencode([ ('field', v) for v in fields ])

        response = self.fetch('/values/?' + queryargs)
        self.assertEqual(response.code, 200)
        self.assertFalse(response.error)

        body = json_decode(response.body)
        self.assertTrue(body['status']['success'])
        self.assertEqual(body['status']['code'], 200)
        body_response = body['response']
 
        ts = time.time()
        for pvname in self.pvnames:
            self.assertIn(pvname, body_response)
            pvvalues = body_response[pvname]
            self.assertEqual( len(pvvalues), 1)
            last_value = pvvalues[0]
            self.assertItemsEqual( last_value.keys(), fields )
            self.assertAlmostEqual(ts, last_value['timestamp'], delta=2)



