import asyncio
import argparse
import sys

# Install asyncio loop integration with tornado
from tornado.platform.asyncio import AsyncIOMainLoop
AsyncIOMainLoop().install()

from tornado.web import Application
from tornado.websocket import WebSocketHandler

from .consumer import subscribe, unsubscribe, consume_message

URL = "amqp://guest:guest@127.0.0.1:5672/"

class EventsWebSocket(WebSocketHandler):
    def initialize(self):
        self.broker_url = self.application.broker_url

    def open(self):
        self.event = asyncio.Event()

    @asyncio.coroutine
    def subscribe(self):
        subscription = yield from subscribe(url=self.broker_url)

        while not self.event.is_set():
            message = yield from consume_message(subscription)
            print("2", message)
            self.write_message(message)

        yield from unsubscribe(subscription)

    def on_message(self, message):
        asyncio.Task(self.subscribe())

    def on_close(self):
        self.event.set()

def make_app(debug=True, broker_url="amqp://guest:guest@127.0.0.1:5672/"):
    application = Application([(r"/", EventsWebSocket)], debug=debug)
    application.broker_url = broker_url

    return application

def start_app(application, *, port=8888, join=True):
    application.listen(port)
    print("Now listening on: http://127.0.0.1:8888", file=sys.stderr)

    if join:
        loop = asyncio.get_event_loop()
        loop.run_forever()

def main():
    parser = argparse.ArgumentParser(description='Taiga.io events-consumer gateway.')
    parser.add_argument("-p", "--port", dest="port", action="store", type=int,
                        default=8888, help="Set custom port number.")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="Run with debug mode activeted")

    args = parser.parse_args()
    app = make_app(debug=args.debug)
    return start_app(app, port=args.port)
