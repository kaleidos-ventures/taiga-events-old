import abc


class WebSocketConnection(object):
    """
    Simple wrapper that works as abstraction for
    websocket connection.
    """

    @property
    def remote_ip(self):
        return self.handler.request.remote_ip

    def __init__(self, handler):
        self.handler = handler

    def write(self, message:str):
        return self.handler.write_message(message)

    def close(self):
        return self.handler.close()


class WebSocketHandler(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_initialize(self, config:dict):
        pass

    @abc.abstractmethod
    def on_open(self, ws):
        pass

    @abc.abstractmethod
    def on_message(self, ws, message:str):
        pass

    @abc.abstractmethod
    def on_close(self, ws):
        pass


# import asyncio

# class KeepAliveWebSocketHandler(WebSocketHandler, metaclass=abc.ABCMeta):
#     def on_initialize(self, config):
#         self.last_pong = time.time()
#         self.keepalive_loop

#     def _keepalive_ventilator(self):
#         while True:
#             yield from asyncio.sleep(2)
#             self.ws.ping()

#     @abc.abstractmethod
#     def on_stop(self, ws):
#         pass

