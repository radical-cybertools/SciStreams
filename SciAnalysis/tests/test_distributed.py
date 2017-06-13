from SciAnalysis.interfaces.StreamDoc import Stream, StreamDoc
from SciAnalysis.interfaces.dask import scatter, gather

# 1. input/output checking?
# 2. retrieving results? re/running with better parameters?


from distributed import Client
client = Client("10.11.128.3:8786")

def infer(data, database="learned-bank.dat"):
    # some logic for image inference
    # for example, read an image
    img = data['img']

    result = dict()
    result['tag'] = "cat"
    return result

def add3(a,b,c=0):
    return a + b + c

def inc1(a):
    return a + 1

s = Stream()
sout = scatter(s)
sout = sout.map(add3)
sout = sout.map(inc1)
sout = gather(sout)
sout.map(print)

s_ml = Stream()
sout_ml = s_ml.map(infer)
sout_ml.map(print)
#sout.map(gather).map(print)

sdoc = StreamDoc(args=(1,2),kwargs=dict(c=3))
s.emit(sdoc)

import numpy as np
img = np.random.random((100,100))
s_ml.emit(dict(img=img))

