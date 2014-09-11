import abc


class WebSocketConnection(object):
    """
    Simple wrapper that works as abstraction for
    websocket connection.
    """

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

