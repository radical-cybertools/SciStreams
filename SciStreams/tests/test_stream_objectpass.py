# test passing an object
from dask import delayed, compute
# for testing the caching
from dask.delayed import tokenize
from dask.base import normalize_token

from SciStreams.interfaces.streams import Stream
from SciStreams.interfaces.StreamDoc import StreamDoc, psdm


def test_object_hash():
    # test that object hashing is working
    class Foo:
        a = 1
        b = 2

    @normalize_token.register(Foo)
    def tokenize_foo(self):
        return normalize_token((self.a, self.b))

    global_list = list()
    def add(foo):
        global_list.append(1)
        return foo.a + foo.b

    myobj = Foo()

    s = Stream()
    # when delayed, should cache
    s.map(delayed(psdm(add))).map(compute)

    s2 = Stream()
    s2.map(psdm(add))

    s.emit(StreamDoc(args=myobj))
    s.emit(StreamDoc(args=myobj))
    assert global_list == [1]

    s2.emit(StreamDoc(args=myobj))
    s2.emit(StreamDoc(args=myobj))
    assert global_list == [1,1,1]
