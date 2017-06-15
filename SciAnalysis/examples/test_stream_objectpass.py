# test passing an object
from dask import delayed, compute
from SciAnalysis.interfaces.StreamDoc import StreamDoc, parse_streamdoc
from functools import wraps

from SciAnalysis.interfaces.StreamDoc import delayed_wrapper
from dask.delayed import tokenize


class Foo:
    a = 1
    b = 2

from dask.base import normalize_token
@normalize_token.register(Foo)
def tokenize_foo(self):
    return normalize_token((self.a, self.b))

def add(foo):
    return foo.a + foo.b

myobj = Foo()

from interfaces.streams import Stream
s = Stream(wrapper=delayed_wrapper)
s.map((add)).apply(compute).apply(print)
s.map(add).apply(print)

s.emit(delayed(StreamDoc(args=myobj), pure=True))
