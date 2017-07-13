from SciAnalysis.interfaces.streams import Stream, stream_accumulate
from SciAnalysis.interfaces.StreamDoc import StreamDoc


class myClass:
    def __init__(self, num):
        self.num = num


@stream_accumulate.register(myClass)
def wrapper(obj, f, **kwargs):
    print("in wrapper")
    res = f(obj.num)
    numout = myClass(res)
    return numout


def addfunc(prev, next, **kwargs):
    print("prev : {}".format(prev))
    print("next : {}".format(next))
    return next + prev

def donothing(*args, **kwargs):
    print('do nothing , args : {}, kwargs : {}'.format(args, kwargs))


s = Stream()
s.accumulate(addfunc).map(print)


s.emit(StreamDoc(args=[1]))#, kwargs=dict(foo="bar"), attributes = dict(name="john")))
s.emit(StreamDoc(args=[2]))#, kwargs=dict(foo="bar"), attributes = dict(name="john")))
s.emit(StreamDoc(args=[3]))#, kwargs=dict(foo="bar"), attributes = dict(name="john")))
s.emit(StreamDoc(args=[4]))#, kwargs=dict(foo="bar"), attributes = dict(name="john")))
