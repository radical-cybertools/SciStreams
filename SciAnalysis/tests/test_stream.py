# tests the stream library
from ..interfaces.streams import Stream, stream_map


def test_stream_map():
    def addfunc(arg):
        return arg+1

    s = Stream()
    sout = s.map(addfunc)
    L = list()
    sout.sink(L.append)

    s.emit(3)
    s.emit(4)

    assert L == [4, 5]


def test_stream_map_wrapper():
    ''' Testing stream mapping with wrappers.
        Create a class which stores number in num.
        Map operations on num rather than the class itself.
    '''
    # define the class to act the wrapper on
    class myClass:
        def __init__(self, num):
            self.num = num

    # register the wrapper for the class
    @stream_map.register(myClass)
    def wrapper(f, obj, **kwargs):
        res = f(obj.num)
        numout = myClass(res)
        return numout

    def addfunc(arg, **kwargs):
        return arg + 1

    s = Stream()
    sout = s.map(addfunc)
    # get the number member
    sout = sout.map(lambda x: x.num, raw=True)

    # save to list
    L = list()
    sout.map(L.append)

    s.emit(myClass(2))
    s.emit(myClass(5))

    assert L == [3, 6]
