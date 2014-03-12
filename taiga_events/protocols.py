import abc

class EventsQueueProtocol(object, metaclass=abc.ABCMeta):
    """
    Defines a module for access to queues.
    """

    @abc.abstractmethod
    def subscribe(self, pattern:str, buffer_size=10):
        pass

    @abc.abstractmethod
    def close_subscription(self, subscription):
        pass

    @abc.abstractmethod
    def consume_message(self, subscription):
        pass



class WebSocketConnectionProtocol(object, metaclass=abc.ABCMeta):
    """
    Defines a module for access to socket connection.
    """

    @abc.abstractmethod
    def write(self, message:str):
        pass

    @abc.abstractmethod
    def close(self):
        pass
