from streamz import Stream
from SciStreams.core.StreamDoc import StreamDoc
from SciStreams.core.StreamDoc import merge, psdm, psda


def test_stream_map():
    '''
        Make sure that stream mapping still works with StreamDoc
    '''

    def addfunc(arg, **kwargs):
        return arg + 1

    s = Stream()
    sout = s.map(psdm(addfunc))
    # get the number member from StreamDoc
    # it is currently a Future now
    sout_futures = sout.map(lambda x: x['args'])
    # convert from Future to a result (blocking function)

    def safe_get(x):
        try:
            res = x.result()
        except AttributeError:
            res = x
        return res
    sout_results = sout_futures.map(safe_get)
    # pick the "_arg0" element of the result
    sout_elems = sout_results.pluck(0)

    # save to list
    # L_futures = sout_futures.sink_to_list()
    L_elems = sout_elems.sink_to_list()

    s.emit(StreamDoc(args=[1], kwargs=dict(foo="bar"),
                     attributes=dict(name="john")))
    s.emit(StreamDoc(args=[4], kwargs=dict(foo="bar"),
                     attributes=dict(name="john")))

    # convert the futures to results
    assert L_elems == [2, 5]


def test_stream_accumulate():
    ''' This tests that the dispatching on the streamdoc's accumulate routine
    is working properly.'''

    def myacc(prevstate, newstate):
        return prevstate + newstate

    s = Stream()
    sout = s.accumulate(psda(myacc))

    L = sout.sink_to_list()

    sout.emit(StreamDoc(args=[1]))
    sout.emit(StreamDoc(args=[2]))
    sout.emit(StreamDoc(args=[3]))

    print(L)


def test_merge():
    ''' Test the merging option for StreamDoc's.'''
    s1 = Stream()
    s2 = Stream()

    stot = s1.zip(s2).map(merge)

    L = stot.sink_to_list()

    sdoc1 = StreamDoc(args=[1, 2], kwargs={'a': 1, 'c': 3})
    sdoc2 = StreamDoc(args=[3, 4], kwargs={'b': 2, 'c': 4})
    s1.emit(sdoc1)

    assert len(L) == 0

    s2.emit(sdoc2)

    result_kwargs = L[0]['kwargs']
    result_args = L[0]['args']

    assert result_kwargs['a'] == 1
    assert result_kwargs['b'] == 2
    assert result_kwargs['c'] == 4
    assert result_args == [1, 2, 3, 4]


def test_new_streamdoc():

    # test initialization
    data = dict(a=1, b=2)
    sdoc = StreamDoc(kwargs=data)
    assert sdoc['kwargs']['a'] == 1

    # update from another sdoc
    sdoc2 = StreamDoc(streamdoc=sdoc)
    assert sdoc2['kwargs']['a'] == 1
