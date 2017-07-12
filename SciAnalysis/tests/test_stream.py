# tests the stream library
from nose.tools import assert_raises
from SciAnalysis.interfaces.streams import Stream, stream_map,\
    stream_accumulate


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

    # test a function with multiple args
    def incfunc(arg, addby):
        return arg + addby

    s = Stream()
    # NOTE : this will err if the correct args are not given
    sout = s.map(incfunc, 2)
    L = list()
    sout.sink(L.append)

    s.emit(3)
    s.emit(4)

    assert L == [5, 6]


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


def test_stream_accumulate():
    ''' test the accumulate function and what it expects as input.
    '''
    # define an acc
    def myacc(prevstate, newstate):
        ''' Accumulate on a state and return a next state.

            Note that accumulator could have returned a newstate, nextout pair.
            Howevever, in that case, an initializer needs to be defined.  This
            may be unnecessary overhead.
        '''
        nextstate = newstate + prevstate
        return nextstate

    s = Stream()
    sout = s.accumulate(myacc)
    L = list()
    sout.map(L.append)

    s.emit(1)
    s.emit(1)
    s.emit(4)

    # should not emit on first
    assert L == [1, 2, 6]


def test_stream_accumulate_wrapper():
    ''' test the accumulate function and what it expects as input, for a
    wrapped function

        NOTE : stream_accumulate will just return whatever was given to it upon
        first try
    '''
    class MyClass:
        def __init__(self, num):
            self.num = num

    # register the wrapper for the class
    @stream_accumulate.register(MyClass)
    def stream_accumulate_myclass(prevobj, nextobj, func):
        # unwrap and re-wrap
        return MyClass(func(prevobj.num, nextobj.num))

    def myacc(prevstate, newstate):
        ''' Accumulate on a state and return a next state.

            Note that accumulator could have returned a newstate, nextout pair.
            Howevever, in that case, an initializer needs to be defined.  This
            may be unnecessary overhead.
        '''
        nextstate = newstate + prevstate
        return nextstate

    s = Stream()
    sout = s.accumulate(myacc)
    sout = sout.map(lambda x: x.num)

    L = list()
    sout.map(L.append)

    s.emit(MyClass(1))
    s.emit(MyClass(1))
    s.emit(MyClass(4))

    # should not emit on first
    assert L == [1, 2, 6]

def test_stream_validate():
    def validate_data(x):
        if isinstance(x, dict):
            return True
        return False

    L = list()

    s = Stream()
    s.validate_output = validate_data
    s.map(L.append)

    assert_raises(ValueError, s.emit, 1)
    s.emit(dict(a=1))

    assert L[0]['a'] == 1

    # now try the dict version
    def validate_data(x):
        if isinstance(x, dict):
            return dict(state=True, message="True")
        return dict(state=False, message="False")

    L = list()

    s = Stream()
    s.validate_output = validate_data
    s.map(L.append)

    assert_raises(ValueError, s.emit, 1)
    s.emit(dict(a=1))

    assert L[0]['a'] == 1
