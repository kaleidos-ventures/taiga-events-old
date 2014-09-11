import traceback
import sys
import amqp
import asyncio
import socket

from collections import namedtuple
from urllib.parse import urlparse

from taiga_events.queues import base

## Custom types definition

RabbitQueue = namedtuple("RabbitQueue", ["name", "channel"])
RabbitChannel = namedtuple("RabbitChannel", ["channel"])
RabbitSubscription = namedtuple("RabbitSubscription", ["conn", "rq", "queue", "event"])

## Low level RabbitMQ connection primitives adapted
## for work with asyncio

@asyncio.coroutine
def _make_rabbitmq_connection(*, url):
    parse_result = urlparse(url)

    # Parse host & user/password
    try:
        (authdata, host) = parse_result.netloc.split("@")
    except Exception as e:
        raise RuntimeError("Invalid url") from e

    try:
        (user, password) = authdata.split(":")
    except Exception:
        (user, password) = ("guest", "guest")

    vhost = parse_result.path
    return amqp.Connection(host=host, userid=user,
                           password=password, virtual_host=vhost)

@asyncio.coroutine
def _make_rabbitmq_queue(conn, *, routing_key="", type="fanout", exchange_name="events"):
    """
    Given a connection and routing key, declare new queue and
    new exchange and return it.
    """

    channel = conn.channel()
    channel.exchange_declare(exchange_name, type)

    (queue_name, _, _) = channel.queue_declare(exclusive=True)
    channel.queue_bind(queue_name, exchange_name)

    return RabbitQueue(queue_name, channel)

@asyncio.coroutine
def _make_rabbitmq_channel(conn, *, routing_key="", type="fanout", exchange_name="events"):
    """
    Given a connection and routing key, declare new queue and
    new exchange and return it.
    """
    channel = conn.channel()
    channel.exchange_declare(exchange_name, type)

    return RabbitChannel(channel)

# @asyncio.coroutine
# def _publish_rabbitmq_message(channel, message, exchange_name="events", routing_key=""):
#     (msg, chan) = (amqp.Message(message), channel.channel)
#     chan.basic_publish(msg, exchange_name)

# @asyncio.coroutine
# def _consume_rabbitmq_messages(conn, queue, callback):
#     assert isinstance(queue, RabbitQueue), "queue should be instance of RabbitQueue"
#
#     def _rcv_callback(msg):
#         callback(msg.body)
#
#     (channel, queue_name) = (rq.channel, rq.name)
#     channel.basic_consume(queue_name, _rcv_callback)
#
#     @asyncio.coroutine
#     def _rcvloop():
#         while True:
#             try:
#                 yield from asyncio.sleep(0.5)
#                 conn.drain_events(0.5)
#             except socket.timeout:
#                 pass
#
#     return asyncio.Task(_rcvloop())

@asyncio.coroutine
def _close_rabbitmq_queue(queue):
    """
    Given a RabbitQueue instance, close it and return nothing.
    """
    assert isinstance(queue, RabbitQueue), "queue should be instance of RabbitQueue"

    rchannel = queue.channel
    rchannel.unbind_queue(queue.name)
    rchannel.close()

@asyncio.coroutine
def _close_rabbitmq_channel(channel):
    """
    Given a plain amqp channel, try close it.
    """
    assert isinstance(channel, RabbitChannel), "channel should be instance of RabbitChannel"

    rchannel = channel.channel
    rchannel.close()

@asyncio.coroutine
def _close_rabbitmq_connection(conn):
    conn.close()


## High level interface for consume messages

@asyncio.coroutine
def _subscribe(*, url, buffer_size=10):
    """
    Given rabbitmq connection string as url,
    starts consumer loop and return a subscription
    instance that can be used for consume messages.
    """

    # Message buffer
    queue = asyncio.Queue(buffer_size)
    stop_event = asyncio.Event()

    # RabbitMQ connection
    conn = yield from _make_rabbitmq_connection(url=url)
    rq = yield from _make_rabbitmq_queue(conn)

    @asyncio.coroutine
    def _receive_messages_loop():
        (channel, queue_name) = (rq.channel, rq.name)

        # TODO: possible bug
        receive_cb = lambda m: asyncio.async(queue.put(m.body))
        channel.basic_consume(queue_name, callback=receive_cb)

        try:
            while not stop_event.is_set():
                try:
                    yield from asyncio.sleep(1)
                    print("qsize", queue.qsize())
                    conn.drain_events(timeout=0.2)
                except socket.timeout:
                    print("timeout")

        except Exception as e:
            traceback.print_exc(file=sys.stderr)

    def _on_rcvloop_done(*args):
        print("RCVLOOP DONE", args)

    # Run message receiver
    rcvloop = asyncio.Task(_receive_messages_loop())
    rcvloop.add_done_callback(_on_rcvloop_done)
    return RabbitSubscription(conn, rq, queue, stop_event)

@asyncio.coroutine
def _close_subscription(subscription):
    """
    Given a subscription, close a related
    rabbitmq queues and connection.
    """
    (rconn, rq, queue, event) = subscription
    # Set event as resolved
    event.set()

    yield from _close_rabbitmq_queue(rq)
    yield from _close_rabbitmq_connection(rconn)

@asyncio.coroutine
def _consume_message(subscription):
    """
    Given a subscription, try consume one message
    if no message is available on buffer, it blocks
    until new message is received.
    """
    (conn, rq, queue, event) = subscription
    return (yield from queue.get())


class EventsQueue(base.EventsQueue):
    """
    Public abstraction.
    """
    def __init__(self, url):
        self.url = url

    @asyncio.coroutine
    def subscribe(self, pattern:str, buffer_size:int=10):
        return (yield from _subscribe(url=self.url,
                                      buffer_size=buffer_size,
                                      pattern=pattern))

    @asyncio.coroutine
    def close_subscription(self, subscription):
        return (yield from _close_subscription(subscription))

    @asyncio.coroutine
    def consume_message(self, subscription):
        return (yield from _consume_message(subscription))
