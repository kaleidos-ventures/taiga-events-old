import amqp
import asyncio

from consumer import make_rabbitmq_connection

@asyncio.coroutine
def make_rabbitmq_channel(conn, *, routing_key="", type="fanout"):
    """
    Given a connection and routing key, declare new queue and
    new exchange and return it.
    """
    channel = conn.channel()
    channel.exchange_declare("events", type)

    return channel

@asyncio.coroutine
def emiter_loop(*, url):
    conn = yield from make_rabbitmq_connection(url=url)
    chan = yield from make_rabbitmq_channel(conn)

    for x in range(10000):
        print("Sleep...")
        yield from asyncio.sleep(1)
        print("Publish: {}".format(x))
        chan.basic_publish(amqp.Message("{}".format(x)), "events")


if __name__ == "__main__":
    url = "amqp://guest:guest@127.0.0.1:5672/"
    loop = asyncio.get_event_loop()

    t = asyncio.Task(emiter_loop(url=url))
    t.add_done_callback(lambda x: loop.stop())

    loop.run_forever()
