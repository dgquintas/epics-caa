import logging
import json

try:
    from collections import namedtuple 
except ImportError:
    from caa.utils.namedtuple import namedtuple

logger = logging.getLogger('caa')

class SubscriptionMode(object):
    available_modes = []

    @classmethod
    def parse(cls, d):
        mode_name = d['mode']
        try:
            mode_cls = getattr(cls, mode_name)
        except AttributeError:
            raise ValueError("Invalid mode '%s'" % mode_name)
        instance = mode_cls(**d)
        return instance
    @classmethod
    def register(cls, mode):
        cls.available_modes.append(mode)
        setattr(cls, mode.name, mode)

    def as_dict(self):
        raise NotImplemented("Subclasses must implement 'as_dict' method")
 
    def __repr__(self):
        return self.__str__()

class _Monitor(SubscriptionMode):
    __slots__ = ('delta',)
    name = 'Monitor'

    def __init__(self, delta=0.0, **dummy):
        self.delta = delta

    def as_dict(self):
        return {'mode': self.name, 'delta': self.delta}

    def __str__(self):
        return 'Monitor(delta=%s)' % self.delta


class _Scan(SubscriptionMode):
    __slots__ = ('period',)
    name = 'Scan'

    def __init__(self, period=0.0, **dummy):
        self.period = period

    def as_dict(self):
        return {'mode': self.name, 'period': self.period}

    def __str__(self):
        return 'Scan(period=%s)' % self.period

SubscriptionMode.register(_Monitor)
SubscriptionMode.register(_Scan)

# since represents the time of subscription
ArchivedPV = namedtuple('ArchivedPV', 'name, mode, since')
class _APVJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SubscriptionMode):
            return obj.as_dict()
        return json.JSONEncoder.default(self, obj)
ArchivedPV.JSONEncoder = _APVJSONEncoder


