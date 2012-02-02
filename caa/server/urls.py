from handlers.welcome import WelcomeHandler
from handlers.controller import *

url_patterns = [
    # args: pvname, mode=Monitor, [params to the mode]
    (r"/subscribe", SubscriptionHandler),
    # args: pvname
    (r"/unsubscribe", UnsubscriptionHandler),
    # args: pvname
    (r"/info", InfoHandler),
    # args: pvname, [pvname, ...]
    (r"/status", StatusHandler),

    # args: pvname, [limit=int, from_date=epoch_secs, to_date=epoch_secs]
    (r"/values", ValuesHandler),
    (r"/config/save", ConfigHandlerSave),
    (r"/config/load", ConfigHandlerLoad),
    (r"/shutdown", ShutdownHandler),
]
