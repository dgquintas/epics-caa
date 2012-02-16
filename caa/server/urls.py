from handlers import controller, server 

url_patterns = [
    (r"/subscription", controller.SubscriptionHandler),
    # POST: 
    # create subscription. Body: JSON object with
    # 2 keys: "pvname" and "mode". Mode must be another
    # JSON object containing the description of the mode. 
    #
    # Eg: 
    # {"pvname": "test:double2", "mode": {"mode": "Scan", "period": 1.1}}
    #
    # GET: 
    # Optional query arguments: "pvname" and "mode", both glob patterns. 
    # Subscribed PVs matching the given name and mode will be returned in the
    # following format:
    #{
    #    "results": [
    #        {
    #            "mode": {
    #                "delta": 1.1, 
    #                "max_freq": null, 
    #                "mode": "Monitor"
    #            }, 
    #            "pvname": "test:long2"
    #        }, 
    #        {
    #            "mode": {
    #                "mode": "Scan", 
    #                "period": 1.1
    #            }, 
    #            "pvname": "test:double2"
    #        }
    #    ], 
    #    "success": true
    #}
    # 
    # DELETE: 
    # unsubscribes from a PV by name. 
    #
    # Eg: 
    # {"pvname": "test:double2"}
    
    
    (r"/info", controller.InfoHandler),
    # GET: 
    # requires a "pvname" query argument. Returns information about that PV's subscription. 
    # Eg:
    # {
    #     "results": {
    #         "mode": {
    #             "mode": "Scan", 
    #             "period": 1.1
    #         }, 
    #         "name": "test:double2", 
    #         "since": 1328917326829359
    #     }, 
    #     "success": true
    # }\
    # If the system isn't subscribed to the given PV, "null" is returned in the "results":
    # {
    #    "results": null, 
    #    "success": true
    # }
    
    (r"/status", controller.StatusHandler),
    # GET: requires a PV name passed as a query argument under key "pvname"
    # returns the status of the given PV: 
    # {
    #     "results": {
    #         "connected": <boolean>
    #     }, 
    #     "success": true
    # }
    # 
    # If PV is unknown, 
    # {
    #    "results": null, 
    #    "success": true
    # }
 
    (r"/values", controller.ValuesHandler),
    # GET: 
    # query arguments: 
    # * pvname
    # * [field, ...]
    # * [limit=100]
    # * [from_date]
    # * [to_date]


    (r"/config", controller.ConfigHandler),
    # GET:
    # returns current config
    # 
    # POST:
    # loads the configuration given in the POST body

    (r"/server/info", server.ServerInfoHandler),

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
    (r"/server/status", server.ServerStatusHandler),
]
