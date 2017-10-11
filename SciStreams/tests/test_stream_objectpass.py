# test passing an object
from dask import delayed, compute, get, set_options
# for testing the caching
from dask.base import normalize_token

from streamz import Stream
import SciStreams.core.scistreams as scs
from SciStreams.core.StreamDoc import StreamDoc

set_options(delayed_pure=True)


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

    # first, verify the hashes are the same
    myobj = Foo()
    first = delayed(add)(myobj)
    myobj2 = Foo()
    second = delayed(add)(myobj2)
    assert first.key == second.key

    # next, check it also works in simple streams (use scistreams)
    s = Stream()
    # when delayed, should cache, the second map is in streams not SciStreams
    sout = scs.map(delayed(add), s)
    sout = scs.map(lambda x: compute(x['args'][0], get=get), sout)
    lout = sout.sink_to_list()

    s2 = Stream()
    s2out = scs.map(add, s2)
    l2out = s2out.sink_to_list()

    s.emit(StreamDoc(args=(myobj)))
    s.emit(StreamDoc(args=(myobj)))
    assert global_list == [1]
    assert lout == [1, 1]

    s2.emit(StreamDoc(args=(myobj)))
    s2.emit(StreamDoc(args=(myobj)))
    assert global_list == [1, 1, 1]
    assert l2out == [1, 1]
