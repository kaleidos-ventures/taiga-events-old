import asyncio
from tornado import websocket as tws

from . import types
from . import websocket as ws


def adapt_handler(handler_cls):
    """
    Function that creates the apropiate tornado handler
    adapter for ~:class:`taiga_events.websocket.WebSocketHandler`
    """

    class _tornado_handler_adapter(tws.WebSocketHandler):
        def initialize(self, config):
            self.__connection = ws.WebSocketConnection(self)
            self.__handler = handler_cls()
            self.__handler.on_initialize(config)
            super().initialize()

        def check_origin(self, origin):
            return True

        def open(self):
            self.set_nodelay(True)
            result = self.__handler.on_open(self.__connection)
            if asyncio.iscoroutine(result):
                asyncio.Task(result)

        def on_message(self, message):
            result = self._handler.on_message(self.__connection, message)
            if asyncio.iscoroutine(result):
                asyncio.Task(result)

        def on_close(self):
            result = self.__handler.on_close(self.__connection)
            if asyncio.iscoroutine(result):
                asyncio.Task(result)

    return _tornado_handler_adapter
