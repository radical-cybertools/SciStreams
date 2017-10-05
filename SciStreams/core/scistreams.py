from functools import wraps

from streamz import Stream
import streamz
from SciStreams.core.StreamDoc import psdm, psda, StreamDoc
import SciStreams.core.StreamDoc as StreamDoc_core

def map(func, child, args=(), input_info=None,
        output_info=None, **kwargs):
    # mapping wrapper for StreamDoc's
    # TODO : use input_info and output_info
    return child.map(psdm(func), *args, **kwargs)

def sink(func, child, args=(), input_info=None,
        output_info=None, **kwargs):
    # mapping wrapper for StreamDoc's
    # TODO : use input_info and output_info
    return child.sink(psdm(func), *args, **kwargs)

def accumulate(func, child, args=(), input_info=None,
        output_info=None, **kwargs):
    # mapping wrapper for StreamDoc's
    # TODO : use input_info and output_info
    return child.accumulate(psda(func), *args, **kwargs)

# wrapper functions into a stream
def select(child, *mapping):
    return streamz.map(child, StreamDoc_core.select, *mapping)

def merge(child):
    return streamz.map(child, StreamDoc_core.merge)

def add_attributes(child, **kwargs):
    return streamz.map(child, StreamDoc_core.add_attributes, attributes=kwargs)

def get_attributes(child):
    return streamz.map(child, StreamDoc_core.get_attributes)

def clear_attributes(child):
    return streamz.map(child, StreamDoc_core.clear_attributes)

def to_attributes(child):
    ''' send a function's args and kwargs to attributes, also clearing the args
    and kwargs'''
    return streamz.map(child, StreamDoc_core.to_attributes)

def to_event_stream(child):
    s2 = child.map(StreamDoc_core.to_event_stream, tolist=True).concat()
    return s2

def pack(child):
    s2 = child.map(psdm(StreamDoc_core.pack))
    return s2

def star(f):
    @wraps(f)
    # still pass the kwargs as usual
    def f_new(args, **kwargs):
        return f(*args, **kwargs)
    return f_new

def istar(f):
    @wraps(f)
    def f_new(*args):
        return f(args)
    return f_new