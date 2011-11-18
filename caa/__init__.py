import json
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import pycassa

import logging

logger = logging.getLogger('caa')

class SubscriptionMode:
    """
        :attr:`MONITOR`: Save every value greater than the given delta. 
        If no delta is specified, the PV's ADEL will be used.

        :attr:`SCAN`: Save with a given period.

        See also :meth:`Controller.subscribe`
    """
    MONITOR = 'Monitor'
    SCAN = 'Scan'


class ArchivedPV(object):
    def __init__(self, name, mode, since, scan_period=0, monitor_delta=0):
        self.name = name
        self.mode = mode
        self.scan_period = scan_period
        self.monitor_delta = monitor_delta
        self.since = since
        
    def __repr__(self):
        return "'ArchivedPV(%s)'" % self.name

    def __str__(self):
        s1 = 'PV name: %s, Mode: %s' % (self.name, self.mode)
        if self.mode == SubscriptionMode.MONITOR:
            s2 = '(delta = %f)' % self.monitor_delta
        else:
            s2 = '(period = %f)' % self.scan_period
        return ' '.join([s1,s2])

    def __hash__(self):
        return hash(self.name)

    def __lt__(self, other): 
        return self._name.__lt__(other.name)

    def __le__(self, other): 
        return self._name.__le__(other.name)

    def __eq__(self, other): 
        return self._name.__eq__(other.name)

    def __ne__(self, other): 
        return self._name.__ne__(other.name)

    def __gt__(self, other): 
        return self._name.__gt__(other.name)

    def __ge__(self, other): 
        return self._name.__ge__(other.name)

    class APVJSONEncoder(json.JSONEncoder):
        def default(self, apv):
            if isinstance(apv, ArchivedPV):
                return (('name', apv.name), ('mode', apv.mode),
                        ('scan_period', apv.scan_period), ('monitor_delta', apv.monitor_delta), 
                        ('since', apv.since))
            return json.JSONEncoder(self, apv)


