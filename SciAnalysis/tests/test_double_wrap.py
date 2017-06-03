def test_double_wrap():
    ''' This one tests that delaying and wrapping can be compounded.
        This uses an unresolved PR as of now.
    '''

    from SciAnalysis.interfaces.streams import stream_map, Stream
    from SciAnalysis.interfaces.StreamDoc import StreamDoc, parse_streamdoc
    from dask.delayed import delayed, Delayed
    from dask import compute

    @stream_map.register(StreamDoc)
    def stream_map_streamdoc(obj, func, **kwargs):
        return parse_streamdoc("test")(func)(obj, **kwargs)

    # now add compute
    @stream_map.register(Delayed)
    def stream_map_delayed(obj, func, **kwargs):
        return delayed(func)(obj, **kwargs)
    # now add compute


    def addone(x):
        return x+1

    a = list()
    s = Stream()
    s2 = s.map(addone)
    s3 = s2.map(lambda x : compute(x)[0], raw=True)
    s3.sink(a.append)
    s3.map(print, raw=True)

    data = 10
    s.emit(delayed(StreamDoc(args=[data])))

    assert a[0]['args'][0] == 11
