# tests the stream library
# it's an internal test just to be sure it's doing what's expected
from streamz import Stream


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
    sacc = s.accumulate(myacc)

    # check the start keyword
    sacc2 = s.accumulate(myacc, start=1)
    L = sacc.sink_to_list()

    L2 = sacc2.sink_to_list()

    s.emit(1)
    s.emit(1)
    s.emit(4)

    # print(L)

    # should emit on first
    assert L == [1, 2, 6]

    # should emit on first
    assert L2 == [2, 3, 7]
