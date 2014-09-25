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

RabbitSubscription = namedtuple("RabbitSubscription", ["conn", "queue", "rcvloop"])

## Low level RabbitMQ connection primitives adapted
## for work with asyncio

class ConnectionManager(object):
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)

        return cls.__instance

    def __init__(self, url):
        self.url = url
        self._connection = None
        self._refcounter = 0

    @asyncio.coroutine
    def _make_connection(self):
        parse_result = urlparse(self.url)

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

    def acquire(self):
        self._refcounter += 1
        if not self._connection:
            self._connection = self._make_connection()
        return self._connection

    def release(self, connection):
        if connection != self._connection:
            return connection.close()

        self._refcounter -= 1
        if self._refcounter == 0:
            self._connection.close()
            self._connection = None


class EventsQueue(base.EventsQueue):
    """
    Public abstraction.
    """
    def __init__(self, url):
        self._connections = ConnectionManager(url)

    @asyncio.coroutine
    def make_connection(self):
        if not hasattr(self, "__connection"):
            conn = Connection(self._make_connection())
        else:
            conn = getattr(self, "__connection")
            conn.increment_refcounter()

        setattr(self, "__connection", conn)
        return conn

    @asyncio.coroutine
    def subscribe(self, routing_key:str, buffer_size:int=10):
        # Message buffer
        queue = asyncio.Queue(buffer_size)

        # RabbitMQ connection
        conn = yield from self.make_connection()

        # Run message receiver
        rcvloop = asyncio.Task(self._receive_messages_loop(conn, routing_key, queue))
        return RabbitSubscription(conn, queue, rcvloop)

    @asyncio.coroutine
    def _receive_messages_loop(self, conn, routing_key, queue):
        channel = conn.channel()
        channel.exchange_declare("events", "topic", auto_delete=True)

        queue_name, _, _ = channel.queue_declare(exclusive=True)
        channel.queue_bind(queue_name, "events", routing_key=routing_key)

        def receive_cb(m):
            asyncio.Task(queue.put(json.loads(m.body)))

        try:
            channel.basic_consume(queue_name, callback=receive_cb)

            while True:
                try:
                    yield from asyncio.sleep(1)
                    conn.drain_events(timeout=0.2)
                except socket.timeout:
                    # This events is raised by conn.drain_events
                    # and should be explictly ignored
                    pass

        except asyncio.CancelledError:
            # This happens when browser closes the conection
            # and we should stop a loop when it happens
            pass

        except KeyboardInterrupt:
            # This can happens when user explicitly terminate
            # the execution and should be ignored
            pass

        except Exception:
            log.error("Unhandled exception", exc_info=True)

        finally:
            channel.queue_unbind(queue_name, exchange="events")
            channel.close()

    @asyncio.coroutine
    def close_subscription(self, subscription):
        """
        Given a subscription, close a related
        rabbitmq queues and connection.
        """
        (rconn, queue, rcvloop) = subscription

        def done_callback(n):
            rconn.close()

        rcvloop.cancel()
        rcvloop.add_done_callback(done_callback)

    @asyncio.coroutine
    def consume_message(self, subscription):
        """
        Given a subscription, try consume one message
        if no message is available on buffer, it blocks
        until new message is received.
        """
        return (yield from subscription.queue.get())

