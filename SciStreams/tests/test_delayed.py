from SciStreams.interfaces.streams import Stream
from dask import delayed, compute
#import SciStreams.globals

# necessary to simulate a cluster
from distributed.utils_test import cluster

# TODO : make a simulated cluster
def test_delayed():
    ''' test that delayed sends to cluster
        when a cluster is defined.'''

    with cluster() as (s, [a, b]):
        # TODO :distributed.set default client
        # (so not conflict with globals)
        def testfunc(a):
            print("test")
            return a + 1

        source = Stream()
        sout = source.map(delayed(testfunc))
        sout = sout.map(lambda x : compute(x)[0])

        L = list()
        sout.map(L.append)

        source.emit(1)
        source.emit(2)
        assert L == [2,3]
