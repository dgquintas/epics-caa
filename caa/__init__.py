import json

class ArchivedPV(object):
    def __init__(self, name, mode, scan_period=0, monitor_delta=0):
        self._name = name
        self._mode = mode
        self._scan_period = scan_period
        self._monitor_delta = monitor_delta
        
        self._last_archived = None

    @property
    def name(self):
        return self._name

    @property
    def mode(self):
        return self._mode

    @property
    def scan_period(self):
        return self._scan_period

    @property
    def monitor_delta(self):
        return self._monitor_delta

    @property
    def last_archived(self):
        if self._last_archived:
            return self._last_archived #TODO: make it pretty
        else:
            return 'N/A'

    @last_archived.setter
    def last_archived(self, date):
        self._last_archived = date #FIXME: make it hold a date object

    def __repr__(self):
        return "'ArchivedPV(%s)'" % self._name

    def __str__(self):
        s1 = 'PV name: %s, Mode: %s' % (self.name, self.mode)
        if self._mode == SubscriptionMode.MONITOR:
            s2 = 'Monitor delta = %f' % self._monitor_delta
        else:
            s2 = 'Scan period = %f' % self._scan_period
        s3 = 'Last archived: %s' % self.last_archived
        return '\n'.join([s1,s2,s3])


    def __hash__(self):
        return hash(self._name)

    def __lt__(self, other): 
        return self._name.__lt__(other._name)

    def __le__(self, other): 
        return self._name.__le__(other._name)

    def __eq__(self, other): 
        return self._name.__eq__(other._name)

    def __ne__(self, other): 
        return self._name.__ne__(other._name)

    def __gt__(self, other): 
        return self._name.__gt__(other._name)

    def __ge__(self, other): 
        return self._name.__ge__(other._name)

    class APVJSONEncoder(json.JSONEncoder):
        def default(self, apv):
            if isinstance(apv, ArchivedPV):
                return (('name', apv.name), ('mode', apv.mode),
                        ('scan_period', apv.scan_period), ('monitor_delta', apv.monitor_delta))
            return json.JSONEncoder(self, apv)
