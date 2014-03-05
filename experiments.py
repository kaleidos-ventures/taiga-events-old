import asyncio as aio

@aio.coroutine
def producer(q):
    for x in range(10):
        print("Sleep...")
        yield from aio.sleep(0.5)
        print("Produce {}.".format(x))
        yield from q.put(x)

@aio.coroutine
def iterator_consume(q):
    while True:
        yield from q.get()


@aio.coroutine
def consumer(q):
    for item in iterator_consume(q):
        print("Consumed {}".format(item))


if __name__ == "__main__":
    q = aio.Queue(2)
    t1 = aio.Task(producer(q))
    t2 = aio.Task(consumer(q))

    loop = aio.get_event_loop()

    def done(kk):
        print("done", kk)
        loop.stop()

    t1.add_done_callback(done)

    loop.run_forever()
        
