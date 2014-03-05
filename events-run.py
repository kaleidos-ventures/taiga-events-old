import amqp
import asyncio

from urllib.parse import urlparse

def _make_connection(*, url):
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

    print(host, user, password, vhost)
    return amqp.Connection(host=host, userid=user,
                           password=password, virtual_host=vhost)

def _make_channel(conn, routing_key):
    channel = conn.channel()
    channel.exchange_declare("events", "fanout")

    (queue_name, _, _) = channel.queue_declare(exclusive=True)
    channel.queue_bind(queue_name, "events")
    return (queue_name, channel)

@asyncio.coroutine
def _receive_messages_loop(queue, conn, chan):
    pass


@asyncio.coroutine
def subscribe(*, url, buffer_size=10):
    # Message buffer
    queue = asyncio.Queue(buffer_size)

    # RabbitMQ connection
    conn = _make_connection(url=url)
    chan = _make_channel(conn, None)

    # Run message receiver
    asyncio.Task(_receive_messages_loop(queue, conn, chan))

    while True:
        message = yield from queue.get()
        print("RECEIVED:", message)



if __name__ == "__main__":
    url = "amqp://guest:guest@127.0.0.1:5672/"
    t = asyncio.Task(subscribe(url=url))

    loop = asyncio.get_event_loop()
    loop.run_forever()
