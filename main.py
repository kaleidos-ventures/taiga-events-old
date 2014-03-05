
# Install asyncio loop integration with tornado
from tornado.platform.asyncio import AsyncIOMainLoop
AsyncIOMainLoop().install()

import asyncio

from tornado.web import Application
from tornado.websocket import WebSocketHandler

from consumer import subscribe, unsubscribe, consume_message

URL = "amqp://guest:guest@127.0.0.1:5672/"


class EventsWebSocket(WebSocketHandler):
    def open(self):
        self.event = asyncio.Event()
        print("WebSocket opened")
        
    @asyncio.coroutine
    def subscribe(self):
        subscription = yield from subscribe(url=URL)
        print("1", subscription)

        while not self.event.is_set():
            message = yield from consume_message(subscription)
            print("2", message)
            self.write_message(message)

        yield from unsubscribe(subscription)

    def on_message(self, message):
        self.task = asyncio.Task(self.subscribe())

    def on_close(self):
        self.event.set()
        print("WebSocket closed")


if __name__ == "__main__":
    application = Application([(r"/", EventsWebSocket)])

    print("Now listening on: http://127.0.0.1:8888")

    application.listen(8888)

    asyncio.get_event_loop().run_forever()


