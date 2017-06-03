#from SciAnalysis.interfaces.streams import Stream
#from SciAnalysis.interfaces.streams import map

from streams import Stream, map
from functools import singledispatch
# register as a singledispatch
@map.register(object)
class newmapclass(Stream):
    def __init__(self, func, child, raw=False, **kwargs):
        print("new stream")
        self.func = func
        self.kwargs = kwargs
        self.raw = raw

        Stream.__init__(self, child)

    def update(self, x, who=None):
        return self.emit(self.func(x, **self.kwargs))


# or a new function name added as the following
# the singledispatch wrapper is just to allow this function also eventually be
# renamed
class NewStream(Stream):
    def newmap(self, func, **kwargs):
        return newmapclass(func, self, **kwargs)


# TODO : need to develop a function registry for streams
s = Stream()
s2 = s.map(lambda x : x+1).filter(lambda x : x > 0).map(lambda x : x+1)
s2.newmap(lambda x : x + 1)#.filter(lambda x : x > 0).map(lambda x : x + 1)
s.emit(1)
