# try reading from databroker
import numpy as np
import time
from uuid import uuid4

from SciStreams import cddb

hdr = cddb['be27420d-de59-46ad-bfba-fdf4804ab616']
event_stream = cddb.restream(hdr, fill=True)



from streams import Stream
import streams.core as sc
import shed.event_streams as es

det_key = 'pilatus2M_image'

def addmydata(x):
    data = x[det_key]
    return data + 1

L = list()
s = Stream()
# this time, we pass the stream to event_stream's
# map method
s2 = es.map(lambda x : x[det_key] + 1, s, input_info={det_key : det_key},
            output_info=((det_key, {'dtype' : 'array'}),))

#s3 = es.map(print, s2, input_info={det_key : det_key},
            #output_info=((det_key,{'dtype' : 'array'}),))
#s3 = es.map(L.append, s2, input_info={det_key : det_key},
    #output_info=((det_key,{'dtype' : 'array'}),))


L2 = list()
s3 = es.map(L.append, s2, input_info={det_key : det_key})
# map to regular stream
s4 = sc.map(L2.append, s2)


for namedocpair in event_stream:
    s.emit(namedocpair)

