#!/usr/bin/env python

import tornado.httpserver
from tornado.ioloop import IOLoop
import tornado.web
from tornado.options import options
from tornado import autoreload

import signal

from settings import settings
from urls import url_patterns

from caa import controller

def keyboard_shutdown(signum, frame):
    controller.shutdown()
    IOLoop.instance().stop()

signal.signal(signal.SIGINT, keyboard_shutdown)

class TornadoApp(tornado.web.Application):
    def __init__(self):
        tornado.web.Application.__init__(self, url_patterns, **settings)


def main():
    app = TornadoApp()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    print 'Tornado server started on port %s' % options.port
    if settings['debug']:
        def f():
            controller.shutdown()
            reload(controller)
        autoreload.add_reload_hook(f)
    try:
        IOLoop.instance().start()
    finally:
        controller.shutdown()



if __name__ == "__main__":
    main()
