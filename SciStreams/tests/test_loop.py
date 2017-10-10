from distributed.utils import sync
from tornado.ioloop import IOLoop
from tornado import gen

# this just tests that the usage of IOLoop is as expected

def test_loop():
    L = list()

    @gen.coroutine
    def f(a):
        L.append(a)

    loop = IOLoop()

    sync(loop, f, 1)

    assert L == [1]
