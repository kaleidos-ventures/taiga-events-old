import asyncio
from tornado.websocket import WebSocketHandler

from . import types
from . import protocols
from . import app


class WebSocketConnectionWrapper(protocols.WebSocketConnectionProtocol):
    """
    Simple wrapper that works as abstraction for
    websocket connection.

    At this moment, it works only for native tornado
    `WebSocketHandler`, but in future can be implemented
    for other implementations like: sockjs or socketio.
    """

    def __init__(self, handler:WebSocketHandler):
        self.handler = handler

    def write(self, message:str):
        return self.handler.write_message(message)

    def close(self):
        return self.handler.close()


## Handler definition

class MainHandler(WebSocketHandler):
    """
    This handler wants receive first message
    containing: authentication token, project id and
    object type.

    Simple example of received data:

    {"token": "1233456789qwertyyuuoip",
     "project": "1"}

    If received token is invalid or user is not member
    of any project, websocket is closed.
    """

    def initialize(self):
        self.stop_event = asyncio.Event()

    def on_message(self, raw_message):
        appconf = types.AppConf(self.application.private_key,
                                self.application.repo_conf,
                                self.application.queue_conf)

        connection_wrapper = WebSocketConnectionWrapper(self)

        coro = subscribe(wsconn=connection_wrapper,
                         appconf=appconf,
                         stop_event=self.stop_event)
        asyncio.async(coro)

    def on_close(self):
        self.stop_event.set()

