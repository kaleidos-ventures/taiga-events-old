import traceback
import asyncio

from collections import namedtuple

from ..protocols import EventsQueueProtocol
from ..utils import pg

PgSubscription = namedtuple("PgSubscription", ["pgconn", "rcvloop", "queue"])


@asyncio.coroutine
def _subscribe(*, dsn:str, buffer_size:int, pattern:str):
    """
    Given a postgresql connection string, buffer_size and
    subscription pattern, starts the consumer loop and
    return subscription instance.
    """
    queue = asyncio.Queue(buffer_size)
    cnn = yield from pg.connect(dsn=dsn)

    @asyncio.coroutine
    def _receive_messages_loop():
        with cnn.cursor() as c:
            while True:
                try:
                    yield from c.execute("LISTEN events;")
                    yield from pg.wait_until_ready_read(cnn)
                    cnn.poll()

                    while cnn.notifies:
                        notify = cnn.notifies.pop()
                        print("NOTIFY: {}".format(notify))
                        yield from queue.put(notify.payload)

                except Exception as e:
                    traceback.print_exc()

    rcvloop = asyncio.Task(_receive_messages_loop())
    return PgSubscription(cnn, rcvloop, queue)


@asyncio.coroutine
def _close_subscription(subscription):
    """
    Given a subscription instance, close related
    postgresql resources.
    """
    assert isinstance(subscription, PgSubscription)

    cnn, rcvloop, queue = subscription
    rcvloop.cancel()

    # FIXME: can not call close when waiting events
    cnn.close()


@asyncio.coroutine
def _consume_message(subscription):
    """
    Given a subscription instance, try consume one message.
    If no message is available on queue, it blocks
    the current coroutine until new message is available.
    """
    assert isinstance(subscription, PgSubscription)

    cnn, rcvloop, queue = subscription
    return (yield from queue.get())


class EventsQueue(EventsQueueProtocol):
    """
    Public abstraction.
    """

    def __init__(self, dsn):
        self.dsn = dsn

    @asyncio.coroutine
    def subscribe(self, pattern:str, buffer_size:int=10):
        return (yield from _subscribe(dsn=self.dsn,
                                      buffer_size=buffer_size,
                                      pattern=pattern))
    @asyncio.coroutine
    def close_subscription(self, subscription):
        return (yield from _close_subscription(subscription))

    @asyncio.coroutine
    def consume_message(self, subscription):
        return (yield from _consume_message(subscription))


