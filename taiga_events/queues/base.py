import abc

class EventsQueue(object, metaclass=abc.ABCMeta):
    """
    Defines a module for access to queues.
    """

    @abc.abstractmethod
    def subscribe(self, routing_key:str, buffer_size:int=10):
        pass

    @abc.abstractmethod
    def close_subscription(self, subscription):
        pass

    @abc.abstractmethod
    def consume_message(self, subscription):
        pass

