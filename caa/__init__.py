import logging
import json
from collections import Mapping

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
            instance = mode_cls(**d)
        except AttributeError:
            raise ValueError("Invalid mode '%s'" % mode_name)
        except:
            raise ValueError("Invalid or missing parameters for mode '%s'" % mode_name)
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

    @classmethod
    def is_registered(cls, modename):
        for m in cls.available_modes:
            if modename == m.name:
                return True
        return False

class _Monitor(dict, SubscriptionMode):
    name = 'Monitor'

    def __init__(self, delta, max_freq=None, **dummy):
        """ 
            :param delta: by how much incoming values must differ from the last archived one.
            :param max_freq: maximum archiving frequency. Data arriving faster than this is discarded.
        """
        dict.__init__(self, ( ('mode', self.name), ('delta', delta), ('max_freq', max_freq) ))

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError(name)

    def __str__(self):
        return '%s(delta=%s, max_freq=%s)' % (self.name, self.delta, self.max_freq)


class _Scan(dict, SubscriptionMode):
    name = 'Scan'

    def __init__(self, period, **dummy):
        """
            :param period: archiving period 
        """
        dict.__init__(self, ( ('mode', self.name), ('period', period) ))

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError(name)

    def __str__(self):
        return '%s(period=%s)' % (self.name, self.period)

SubscriptionMode.register(_Monitor)
SubscriptionMode.register(_Scan)


class ArchivedPV(Mapping, dict):
    def __init__(self, name, subscribed, mode=None, since=None, **kwargs):
        self._d = {'name': name, 
                   'subscribed': subscribed,
                   'since': since}
        if mode:
            try:
                mode = json.loads(mode)
            except:
                pass
            self._d['mode'] = SubscriptionMode.parse(mode)

    def __repr__(self):
        return 'ArchivedPV(%r)' % self._d

    def __len__(self):
        return len(self._d)

    def __iter__(self):
        return iter(self._d)

    def __getitem__(self, name):
        return self._d[name]

    def __getattr__(self, name):
        return self[name]

