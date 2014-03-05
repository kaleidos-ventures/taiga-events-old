import traceback
import sys
import amqp
import asyncio
import socket

from collections import namedtuple
from urllib.parse import urlparse

# Define types
RabbitQueue = namedtuple("RabbitQueue", ["name", "channel"])
RabbitSubscription = namedtuple("RabbitSubscription", ["conn", "rq", "queue", "event"])

@asyncio.coroutine
def make_rabbitmq_connection(*, url):
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
def make_rabbitmq_queue(conn, *, routing_key="", type="fanout"):
    """
    Given a connection and routing key, declare new queue and
    new exchange and return it.
    """

    channel = conn.channel()
    channel.exchange_declare("events", "fanout")

    (queue_name, _, _) = channel.queue_declare(exclusive=True)
    channel.queue_bind(queue_name, "events")

    return RabbitQueue(queue_name, channel)

@asyncio.coroutine
def close_rabbitmq_connection(conn):
    conn.close()

@asyncio.coroutine
def close_rabbitmq_queue(queue:RabbitQueue) -> None:
    """
    Given a RabbitQueue instance, close it and return nothing.
    """
    channel = queue.channel
    channel.unbind_queue(queue.name)
    channel.close()

@asyncio.coroutine
def _receive_messages_loop(conn, rq, queue, event):
    (channel, queue_name) = (rq.channel, rq.name)

    receive_cb = lambda m: asyncio.Task(queue.put(m.body))
    channel.basic_consume(queue_name, callback=receive_cb)

    try:
        while not event.is_set():
            try:
                yield from asyncio.sleep(1)
                conn.drain_events(timeout=0.2)
            except socket.timeout:
                pass

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        print("Exception raised:", e.__class__, e)

@asyncio.coroutine
def subscribe(*, url, buffer_size=10):
    # Message buffer
    queue = asyncio.Queue(buffer_size)
    stop_event = asyncio.Event()

    # RabbitMQ connection
    conn = yield from make_rabbitmq_connection(url=url)
    rq = yield from make_rabbitmq_queue(conn)

    def _on_rcvloop_done(*args):
        print("RCVLOOP DONE", args)

    print("KKK")

    # Run message receiver
    rcvloop = asyncio.Task(_receive_messages_loop(conn, rq, queue, stop_event))
    rcvloop.add_done_callback(_on_rcvloop_done)

    return RabbitSubscription(conn, rq, queue, stop_event)

@asyncio.coroutine
def consume_message(subscription):
    (conn, rq, queue, event) = subscription
    return (yield from queue.get())

@asyncio.coroutine
def unsubscribe(subscription):
    (rconn, rq, queue, event) = subscription
    # Set event as resolved
    event.set()

    yield from close_rabbitmq_queue(rq)
    yield from close_rabbitmq_connection(rconn)

@asyncio.coroutine
def consume(*, url):
    subscription = yield from subscribe(url=url)

    while True:
        message = yield from consume_message(subscription)
        print("RECEIVED:", message)

        if message == "9":
            yield from unsubscribe(subscription)
            break

if __name__ == "__main__":
    url = "amqp://guest:guest@127.0.0.1:5672/"
    t = asyncio.Task(consume(url=url))

    loop = asyncio.get_event_loop()
    loop.run_forever()
