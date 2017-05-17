# create a bunch of computations to run on dask, check that store computations
# match number here

from distributed import Client
from dask import delayed

from collections import deque
futures = deque(maxlen=1000)

def testfunction(a):
    return a

client = Client("10.11.128.3:8786")

from SciAnalysis.interfaces.StreamDoc import Stream
s = Stream()
sout = s.map(delayed(testfunction)).map(client.compute).map(futures.append)


for i in range(1000):
    s.emit(i)
    #futures.append(client.compute(delayed(testfunction(i))))
