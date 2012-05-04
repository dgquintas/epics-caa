from tornado.web import URLSpec
from handlers import RootHandler, PVHandler, SubscriptionHandler, ConfigHandler

PVNAME = "[a-zA-Z0-9:_\.]+"

url_patterns = [
    URLSpec(r"/", RootHandler, name="root"),
    URLSpec(r"/subscriptions/", SubscriptionHandler, name="subscriptions"),

    URLSpec(r"/config/?", ConfigHandler, name="config"),

    URLSpec(r"/(?P<pvname>"+PVNAME+")/?", PVHandler, name="pv"),



]
