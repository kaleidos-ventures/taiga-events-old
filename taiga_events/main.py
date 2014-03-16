import asyncio
import argparse
import sys

# Install asyncio loop integration with tornado
from tornado.platform.asyncio import AsyncIOMainLoop
AsyncIOMainLoop().install()

from tornado.web import Application
from .handlers import MainHandler

def make_app(debug:bool=True, secret_key:str=None):
    application = Application([(r"/events", MainHandler)], debug=debug)
    application.secret_key = secret_key

    # TODO: temporary hardcoded
    application.repo_conf = {"kwargs": {"dsn": "dbname=taiga"}}

    # Event source configuration. Initially for rabbitmq.
    application.queue_conf = {"path": "taiga_events.queues.pg.EventsQueue",
                              "kwargs": {"dsn": "dbname=taiga"}}
    return application

def start_app(application, *, port=8888, join=True):
    application.listen(port)
    print("Now listening on: http://127.0.0.1:{0}".format(port), file=sys.stderr)

    if join:
        loop = asyncio.get_event_loop()
        loop.run_forever()

def main():
    parser = argparse.ArgumentParser(description='Taiga.io events-consumer gateway.')
    parser.add_argument("-p", "--port", dest="port", action="store", type=int,
                        default=8888, help="Set custom port number.")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="Run with debug mode activeted")
    parser.add_argument("-s", "--secret-key", dest="secret_key", action="store",
                        required=True, help="Set secret key")

    args = parser.parse_args()
    app = make_app(debug=args.debug, secret_key=args.secret_key)
    return start_app(app, port=args.port)
