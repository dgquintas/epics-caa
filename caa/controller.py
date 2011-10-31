from __future__ import print_function
from caa import datastore
from caa import ArchivedPV

try:
    import ConfigParser as configparser
except:
    import configparser

try:
    from collections import OrderedDict
except:
    from caa.utils.OrderedDict import OrderedDict
import json
#from multiprocessing import cpu_count, Queue, Process
from multiprocessing import cpu_count, Process, current_process, Queue
from threading import Thread , current_thread
#from Queue import Queue
import logging

import epics


"""
.. module:: controller
    :synopsis: Interface to the outside world

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>
"""

logger = logging.getLogger('controller')

class SubscriptionMode:
    """
        :attr:`MONITOR`: Save every value greater than the given delta. 
        If no delta is specified, the PV's ADEL will be used.

        :attr:`SCAN`: Save with a given period.

        See also :meth:`Controller.subscribe`
    """
    MONITOR = 'Monitor'
    SCAN = 'Scan'

class WorkersPool(object):

    STOP_SENTINEL = '__STOP'

    def __init__(self, num_workers=cpu_count()):
        self._task_queue = Queue()
        self._done_queue = Queue()
        self._workers = [ Process(target=self.worker, name=("Worker-%d"%i)) for i in range(num_workers) ]
        

    @property
    def num_workers(self):
        return len(self._workers)

    @property
    def task_queue(self):
        return self._task_queue 

    @property
    def done_queue(self):
        return self._done_queue

    def submit(self, task, args):
        """ Adds a callable (``task``) to the pool.

            :param callable task: a callable object 
            :param list args: arguments to be passed to ``task``
        """
        logger.info("Submitting task '%s' with arguments '%s'", task, args)
        self._task_queue.put((task, args))

    def worker(self):
        logger.debug("At worker function @ %s", current_process())
        for t,args in iter(self._task_queue.get, self.STOP_SENTINEL):
            logger.debug("Worker '%s' processing task '%s'", current_process(), t)
            t_res = t(*args)
            self._done_queue.put(t_res)
        logger.debug("Worker %s exiting", current_process())

    def get_result(self, block=False, timeout=None):
        """ Returns the results of a previously submitted job. 

            :param boolean block: If ``True``, will block until a result in available. Otherwise,
            return an item if one is immediately available, else raise :exc:`Queue.Empty`.
            :param float timeout: If blocking, how many seconds to wait. If no result 
            is available after that many seconds, :exc:`Queue.Empty` exception is raised.
        """
        return self._subspool.done_queue.get(block, timeout)

    def start(self):
        for w in self._workers:
            w.daemon = True
            logger.info("Starting worker %s", w)
            w.start()

    def stop(self):
        logger.info("Stopping workers...")
        for _ in range(self.num_workers):
            self._task_queue.put(self.STOP_SENTINEL)

def subscription_cb(**kw):
    """ Internally called when a new value arrives for a subscribed PV.
    
        :param dict kw: will contain the following information about the PV:

         pvname
             the name of the pv.

         value
             the latest value.

         char_value
             string representation of value.

         count
             the number of data elements

         ftype
             the numerical CA type indicating the data type

         type
             the python type for the data

         status
             the status of the PV (1 for OK)

         precision
             number of decimal places of precision for floating point values

         units
             string for PV units

         severity
             PV severity

         timestamp
             timestamp from CA server.

         read_access
             read access (True or False)

         write_access
             write access (True or False)

         access
             string description of read- and write-access

         host
             host machine and CA port serving PV

         enum_strs
             the list of enumeration strings

         upper_disp_limit
             upper display limit

         lower_disp_limit
             lower display limit

         upper_alarm_limit
             upper alarm limit

         lower_alarm_limit
             lower alarm limit

         upper_warning_limit
             upper warning limit

         lower_warning_limit
             lower warning limit

         upper_ctrl_limit
             upper control limit

         lower_ctrl_limit
             lower control limit

         chid
             integer channel ID

         cb_info
             (index, self) tuple containing callback ID and the PV object

    """
    # store in DB
    #assert 'pvname' in kw
    #self._datastore.write( kw['pvname'], kw )
    print("PV %(pvname)s changed to value %(value)s" % kw)

def epics_subscribe(pvname):
    logger.info("EPICS-subscribing to %s", pvname)
    pv = epics.PV(pvname, callback=subscription_cb)
    return pv.pvname


class Controller(object):
    def __init__(self, keyspace, column_family):
        self._subs = {} #values being ArchivedPV instances

        self._datastore = datastore.DataStore(keyspace, column_family)

        self._subspool = WorkersPool()
        self._subspool.start()

        self._pvs = []

    def subscribe(self, pv_name, mode=SubscriptionMode.MONITOR, scan_period=0, monitor_delta=0):
        """ Adds the PV to the list of PVs to be archived """
        
        archived_pv = ArchivedPV(pv_name, mode, scan_period, monitor_delta)
        self._subs[pv_name] = archived_pv

        # submit subscription request to queue
        self._subspool.submit(epics_subscribe, (pv_name, ))
        #_epics_subscribe(epics.PV, pv_name)


    def unsubscribe(self, pv_name):
        """ Stops archiving the PV """
        #FIXME: complain if missing key?
        if pv_name in self._subs:
            del self._subs[pv_name]

    def get_info(self, pv_name):
        """ Returns an :class:`ArchivedPV` instance for the given subscribed PV name.

            If there's no subscription to that PV, ``None`` is returned.
        """
        return self._subs.get(pv_name, None)

    @property
    def subscribed_pvs(self):
        """ The sorted list of all PV *names* being archived (any mode) 
        
            See also: :class:`SubscriptionMode`
        """
        res = sorted( pv._name for pv in self._subs.values() )
        return res

    @property
    def monitored_pvs(self):
        """ The sorted list of *monitored* PV names.
        
            See also: :class:`SubscriptionMode`
        """
        res = sorted( pv._name for pv in self._subs.values() if pv._mode == SubscriptionMode.MONITOR )
        return res

    @property
    def scanned_pvs(self):
        """ The sorted list of *scanned* PV names.
            
            See also: :class:`SubscriptionMode`
        """ 
        res = sorted( pv._name for pv in self._subs.values() if pv._mode == SubscriptionMode.SCAN )
        return res


    def get_value(self, pv_name):
        """ Returns latest archived data for the PV """
        pass

    def get_values(self, pv_names_list):
        """ Returns the latest archived data for the given PVs.

            This method is more efficient that individual calls to
            :meth:`get_value`. 

            :rtype: a dictionary keyed by the PV name.
        """
        return self._datastore.read(pv_names_list)
        

    def load_config(self, fileobj):
        """ Restore the state defined by the config """
        for line in fileobj:
            d = dict(json.loads(line))
            self.subscribe(d['name'], d['mode'], d['scan_period'], d['monitor_delta'])


    def save_config(self, fileobj):
        """ Save current subscription state. """
        # get a list of the ArchivedPV values 
        for archived_pv in self._subs.values():
            fileobj.write(json.dumps(archived_pv, cls=ArchivedPV.APVJSONEncoder) + '\n')
        

    def shutdown(self):
        self._subspool.stop()

    def __del__(self):
        self.shutdown()










