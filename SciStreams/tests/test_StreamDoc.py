from SciStreams.interfaces.streams import Stream
from SciStreams.interfaces.StreamDoc import StreamDoc


def test_stream_map():
    '''
        Make sure that stream mapping still works with StreamDoc
    '''

    def addfunc(arg, **kwargs):
        return arg + 1

    s = Stream()
    sout = s.map(addfunc)
    # get the number member from StreamDoc
    sout = sout.map(lambda x: x['args'][0], raw=True)

    # save to list
    L = list()
    sout.map(L.append)

    s.emit(StreamDoc(args=[1], kwargs=dict(foo="bar"),
                     attributes=dict(name="john")))
    s.emit(StreamDoc(args=[4], kwargs=dict(foo="bar"),
                     attributes=dict(name="john")))

    assert L == [2, 5]

def test_stream_accumulate():
    ''' This tests that the dispatching on the streamdoc's accumulate routine
    is working properly.'''


    def myacc(prevstate, newstate):
        return prevstate + newstate

    s = Stream()
    sout = s.accumulate(myacc)

    L = list()
    sout.map(L.append)

    sout.emit(StreamDoc(args=[1]))
    sout.emit(StreamDoc(args=[2]))
    sout.emit(StreamDoc(args=[3]))

    print(L)
