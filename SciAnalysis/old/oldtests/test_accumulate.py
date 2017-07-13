# testing accumulate
from SciAnalysis.interfaces.StreamDoc import StreamDoc
from SciAnalysis.interfaces.streams import Stream, stream_accumulate

def test_accumulate():
    def accumulator(a,b):
        return a + b

    @stream_accumulate.register(int)
    def accumulate_wrap(prev,next,f):
        print("no wrap")
        return f(prev,next)


    s = Stream()

    s2 = s.accumulate(accumulator)
    #s2.map(print)
    s.map(print)


    sdoc = StreamDoc(args=[1])
    s.emit(1)
    s.emit(1)
    s.emit(1)
    #s.emit(sdoc)
    #s.emit(sdoc)
    #s.emit(sdoc)
