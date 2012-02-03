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
        """ Creates an instance from the description in dictionary `d`.
        
            This operation is the complementary of :meth:`as_dict`
        """
        mode_name = d['mode']
        try:
            mode_cls = getattr(cls, mode_name)
        except AttributeError:
            raise ValueError("Invalid mode '%s'" % mode_name)
        instance = mode_cls(**d)
        return instance
    @classmethod
    def register(cls, mode):
        """ Register `mode`, a subclass of `SubscriptionMode`. 
        
            This makes the mode subclass accessible as 
            `SubscriptionMode.<modename>`. For instance, `SubscriptionMode.Monitor` 
            for a mode subclass named 'Monitor' 
        """
        cls.available_modes.append(mode)
        setattr(cls, mode.name, mode)

    def __eq__(self, other):
        return self == other

    def __repr__(self):
        return self.__str__()

class _Monitor(dict, SubscriptionMode):
    name = 'Monitor'

    def __init__(self, delta=0.0, max_freq=None, **dummy):
        """ 
            :param delta: by how much incoming values must differ from the last archived one.
            :param max_freq: maximum archiving frequency. Data arriving faster than this is discarded.
        """
        dict.__init__(self, ( ('mode', self.name), ('delta', delta), ('max_freq', max_freq) ))

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            super(_Monitor, self).__getattr__(name)

    def __str__(self):
        return '%s(delta=%s, max_freq=%s)' % (self.name, self.delta, self.max_freq)


class _Scan(dict, SubscriptionMode):
    name = 'Scan'

    def __init__(self, period=0.0, **dummy):
        """
            :param period: archiving period 
        """
        dict.__init__(self, ( ('mode', self.name), ('period', period) ))

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            super(_Scan, self).__getattr__(name)

    def __str__(self):
        return '%s(period=%s)' % (self.name, self.period)

SubscriptionMode.register(_Monitor)
SubscriptionMode.register(_Scan)

# since represents the time of subscription
#ArchivedPV = namedtuple('ArchivedPV', 'name, mode, since')
#class _APVJSONEncoder(json.JSONEncoder):
#    def default(self, obj):
#        if isinstance(obj, SubscriptionMode):
#            return obj.as_dict()
#        return json.JSONEncoder.default(self, obj)
#ArchivedPV.JSONEncoder = _APVJSONEncoder

class ArchivedPV(dict):
    __slots__ = ()
    def __init__(self, name, mode, since):
        dict.__init__(self, ( ('name', name), ('mode', mode), ('since', since) ))

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            super(ArchivedPV, self).__getattr__(name)






