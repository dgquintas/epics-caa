from handlers import controller, server 
from tornado.web import URLSpec

url_patterns = [
    URLSpec(r"^/subscriptions/?$", controller.RootSubscriptionHandler, name='subs-root'),
    # GET: List of subscribed pvs
    # Optional query arguments: 
    #   pvname: only pvs matching this glob
    #   mode: only pvs with this mode (refer to the mode by name)
    
    # PUT:
    # Replace current subscriptions with the given set. 
    # Body:
    # [ {pvname: <pvname_1>, mode: <parseable mode_1 definition>}, 
    #   ...
    #   {pvname: <pvname_n>, mode: <parseable mode_n definition>} ]
 
    # POST:
    # Create a new subscription. The url of the new subscribed pv is returned. 
    # Body: 
    # {pvname: <pvname>, mode: <parseable mode definition>} 

    # DELETE:
    # unsubscribe from all pvs
    
    URLSpec(r"^/subscriptions/(?P<pvname>.+)$", controller.PVSubscriptionHandler, name='subs-pv'),
    # GET: 
    # Return information on the PV's subscription. 
    # Eg:
    # {
    #     "results": {
    #         "mode": {
    #             "mode": "Scan", 
    #             "period": 1.1
    #         }, 
    #         "name": "test:double2", 
    #         "since": 1328917326829359,
    #     }, 
    #     "success": true
    # }
    # If the system isn't subscribed to the given PV, "null" is returned in the "results":
    # {
    #    "results": null, 
    #    "success": true
    # }
 
    # PUT:
    # replace subscription if already exists. Create it otherwise.
    # Body: 
    # {"mode": {"mode": "Scan", "period": 1.1}}
 
    # DELETE: 
    # unsubscribes from the pv


    (r"^/statuses/?$", controller.RootStatusesHandler),
    # GET:
    # Return the status of all the subscribed PVs
    # query arguments:
    # pvname: name glob
    # limit: number of status entries to return per PV. 1 (latest) by default

    (r"^/statuses/(?P<pvname>.+)$", controller.PVStatusesHandler),
    # GET:
    # Return the status of the given PV
    # query arguments: 
    # limit: number of status entries to return. 10 by default

    (r"^/values/?$", controller.RootValuesHandler),
    # GET: 
    # Return the last <limit> (default 1) values for all subscribed PV's. If <pvname>
    # glob present, return only those PV's whose name match it.
    # If one or more <field> is given, return only those fields from the
    # PV values data.
    # 
    # query arguments: 
    # * pvname: name glob
    # * [field, ... ]
    # * [limit=1]
    
    (r"^/values/(?P<pvname>.+)$", controller.PVValuesHandler),
    # GET:
    # Return values for <pvname>.
    #
    # query arguments:
    # [field, ...]
    # [limit=100]
    # [from_date]
    # [to_date]


    (r"^/config$", controller.ConfigHandler),
    # GET:
    # returns current config
    # 
    # PUT:
    # loads the configuration given in the POST body


##################################

    (r"^/server/info$", server.ServerInfoHandler),
    # GET: Return information about the server

    (r"^/server/status$", server.ServerStatusHandler),
    # POST: if "shutdown" is provided in the body of the POST, well,
    # guess what will happen
    # GET: returns the status of the server in the following fashion
    #        d = {'status': 
    #              {'workers': <boolean>
    #               'timers' : <boolean>},
    #        'number_of': 
    #            {'workers': <int>,
    #             'timers' : <int>,
    #             'pvs'    : <int>}
    #        }

]
