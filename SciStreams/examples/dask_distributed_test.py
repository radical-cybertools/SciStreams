# create a bunch of computations to run on dask, check that store computations
# match number here

from distributed import Client
from dask import delayed

from collections import deque
futures = deque(maxlen=1000)

def testfunction(a):
    return a

client = Client("10.11.128.3:8786")

from time import sleep
def mysleepfunc(a):
    sleep(2)

from SciAnalysis.interfaces.StreamDoc import Stream
s = Stream()
sout = s.map(delayed(testfunction)).map(delayed(mysleepfunc)).map(client.compute)
sout.map(futures.append)

#sout = sout.map(client.gather)
#sout.map(print)


for i in range(10):
    s.emit(i)
    #futures.append(client.compute(delayed(testfunction(i))))
print('waiting')


s = Stream()
sout = streams.dask.scatter(s)
evenbranch = sout.filter(lambda x : x % 2 == 0)
oddbranch = sout.filter(lambda x : x % 2 == 1)
# apply different transformations to even or odd
evenbranch.map(lambda x : x**2)
oddbranch.map(lambda x : x**3)


inputs = [1,0,1,1,0,0,0,1]
for x in inputs:
    s.emit(x)
