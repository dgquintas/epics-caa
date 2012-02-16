from handlers.base import BaseHandler
from tornado.ioloop import IOLoop

from caa import controller, SubscriptionMode

import logging
import resource

logger = logging.getLogger('server.caa.' + __name__)

class ServerInfoHandler(BaseHandler):

    @staticmethod
    def _gather_res_info(rusage_struct):
        d = {'RSS': rusage_struct.ru_maxrss, 'usertime': rusage_struct.ru_utime, 'systemtime': rusage_struct.ru_stime}
        return d

    def get(self):
        rself = resource.getrusage(resource.RUSAGE_SELF)
        rchildren = resource.getrusage(resource.RUSAGE_CHILDREN)

        self.win( {'self': self._gather_res_info(rself), \
                   'workers': self._gather_res_info(rchildren)} )

class ServerStatusHandler(BaseHandler):

    def _shutdown(self):
        """ Fully stops the archiver and this server """
        controller.shutdown()
        self.win("Shutting down right away")
        ioloop = IOLoop.instance()
        ioloop.stop()


    def get(self):
        num_workers = controller.workers.num_workers
        num_timers  = controller.timers.num_timers
        num_pvs = sum(1 for _ in controller.get_pvs())

        d = {'status': 
                {'workers': controller.workers.running,
                 'timers' : controller.timers.running
                },
            'number_of': 
                {'workers': num_workers,
                 'timers' : num_timers,
                 'pvs'    : num_pvs
                }
            }
        self.win(d)

    def post(self):
        if self.request.body.strip() == "shutdown":
            self._shutdown()

