from SciAnalysis.interfaces.streams import Stream, stream_map
from SciAnalysis.interfaces.StreamDoc import StreamDoc


class myClass:
    def __init__(self, num):
        self.num = num


@stream_map.register(myClass)
def wrapper(obj, f, **kwargs):
    print("in wrapper")
    res = f(obj.num)
    numout = myClass(res)
    return numout


def addfunc(arg, **kwargs):
    return arg + 1


def donothing(*args, **kwargs):
    print('do nothing , args : {}, kwargs : {}'.format(args, kwargs))


s = Stream()
s.map(donothing)
#s.map(addfunc).map(print, raw=True)
s.emit(myClass(2))
s.emit(myClass(5))


s.emit(StreamDoc(kwargs=dict(foo="bar"), attributes = dict(name="john")))
