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

    def as_dict(self):
        """ Abstract method. Must return a serializable representation 
            of the instance.

            The keys must correspond to parameters of the subclass' constructor,
            as seen in :meth:`parse .
        """
        raise NotImplemented("Subclasses must implement 'as_dict' method")
 
    def __repr__(self):
        return self.__str__()

class _Monitor(SubscriptionMode):
    __slots__ = ('delta','max_freq')
    name = 'Monitor'

    def __init__(self, delta=0.0, max_freq=None, **dummy):
        """ 
            :param delta: by how much incoming values must differ from the last archived one.
            :param max_freq: maximum archiving frequency. Data arriving faster than this is discarded.
        """
        self.delta = delta
        self.max_freq = max_freq

    def as_dict(self):
        return {'mode': self.name, 'delta': self.delta, 'max_freq': self.max_freq}

    def __str__(self):
        return 'Monitor(delta=%s, max_freq=%s)' % (self.delta, self.max_freq)


class _Scan(SubscriptionMode):
    __slots__ = ('period',)
    name = 'Scan'

    def __init__(self, period=0.0, **dummy):
        """
            :param period: archiving period 
        """
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


