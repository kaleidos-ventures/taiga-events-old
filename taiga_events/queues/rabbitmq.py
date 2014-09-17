import traceback
import sys
import amqp
import asyncio
import socket
import logging
import json

from collections import namedtuple
from urllib.parse import urlparse

from taiga_events.queues import base

log = logging.getLogger("taiga.rabbitmq")

## Custom types definition

RabbitQueue = namedtuple("RabbitQueue", ["name", "channel"])
RabbitChannel = namedtuple("RabbitChannel", ["channel"])
RabbitSubscription = namedtuple("RabbitSubscription", ["conn", "rq", "queue", "rcvloop"])

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
def _make_rabbitmq_queue(conn, *, routing_key:str, type:str="topic", exchange_name:str="events"):
    """
    Given a connection and routing key, declare new queue and
    new exchange and return it.
    """

    channel = conn.channel()
    channel.exchange_declare(exchange_name, type, auto_delete=True)

    (queue_name, _, _) = channel.queue_declare(exclusive=True)
    channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

    return RabbitQueue(queue_name, channel)


@asyncio.coroutine
def _close_rabbitmq_queue(queue):
    """
    Given a RabbitQueue instance, close it and return nothing.
    """
    assert isinstance(queue, RabbitQueue), "queue should be instance of RabbitQueue"

    rchannel = queue.channel
    rchannel.queue_unbind(queue.name, exchange="events")
    rchannel.close()


@asyncio.coroutine
def _close_rabbitmq_connection(conn):
    conn.close()


## High level interface for consume messages

@asyncio.coroutine
def _subscribe(routing_key, *, url, buffer_size=10):
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
    rq = yield from _make_rabbitmq_queue(conn, routing_key=routing_key)

    @asyncio.coroutine
    def _receive_messages_loop():
        (channel, queue_name) = (rq.channel, rq.name)

        receive_cb = lambda m: asyncio.Task(queue.put(m.body))

        def receive_cb(m):
            log.debug("RabbitMQ message received: %s", m.body)
            asyncio.Task(queue.put(json.loads(m.body)))

        channel.basic_consume(queue_name, callback=receive_cb)

        while True:
            try:
                yield from asyncio.sleep(1)
                # print("qsize", queue.qsize())
                conn.drain_events(timeout=0.2)
            except socket.timeout:
                # This events is raised by conn.drain_events
                # and should be explictly ignored
                continue

            except asyncio.CancelledError:
                # This happens when browser closes the conection
                # and we should stop a loop when it happens
                break

            except KeyboardInterrupt:
                # This can happens when user explicitly terminate
                # the execution and should be ignored
                log.error("Keyboard interrupt")
                break

            except Exception:
                log.error("Unhandled exception", exc_info=True)
                break

    # Run message receiver
    rcvloop = asyncio.Task(_receive_messages_loop())
    return RabbitSubscription(conn, rq, queue, rcvloop)

@asyncio.coroutine
def _close_subscription(subscription):
    """
    Given a subscription, close a related
    rabbitmq queues and connection.
    """
    (rconn, rq, queue, rcvloop) = subscription
    rcvloop.cancel()

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
        log.debug("RabbitMQ queue instanciated")

    @asyncio.coroutine
    def subscribe(self, routing_key:str, buffer_size:int=10):
        log.debug("RabbitMQ::subscribe called with %s", routing_key)
        return (yield from _subscribe(routing_key,
                                      url=self.url,
                                      buffer_size=buffer_size))
    @asyncio.coroutine
    def close_subscription(self, subscription):
        return (yield from _close_subscription(subscription))

    @asyncio.coroutine
    def consume_message(self, subscription):
        return (yield from _consume_message(subscription))
