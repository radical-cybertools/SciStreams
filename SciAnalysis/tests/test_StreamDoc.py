from ..interfaces.streams import Stream
from ..interfaces.StreamDoc import StreamDoc


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
