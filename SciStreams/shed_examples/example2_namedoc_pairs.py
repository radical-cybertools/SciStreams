import numpy as np
import time
from uuid import uuid4
def gen_imgs(data, **md):
    run_start = str(uuid4())
    yield 'start', dict(uid=run_start, time=time.time(), **md)
    des_uid = str(uuid4())

    yield 'descriptor', dict(run_start=run_start, data_keys={
        'data': dict(
            source='testing', dtype='array')}, time=time.time(), uid=des_uid)
    for i, datum in enumerate(data):
        yield 'event', dict(descriptor=des_uid,
                            uid=str(uuid4()),
                            time=time.time(),
                            data={'data': datum},
                            timestamps={'data': time.time()},
                            filled={'data': True},
                            seq_num=i)
    yield 'stop', dict(run_start=run_start,
                       uid=str(uuid4()),
                       time=time.time())

data1 = np.ones((100,100))
data2 = np.ones((100,100))
event_stream = gen_imgs([data1, data2], name="Alex", sample="FeO")

from streams import Stream
import shed.event_streams as es

def addmydata(x):
    data = x['data']
    return data + 1

s = Stream()
# this time, we pass the stream to event_stream's
# map method
s2 = es.map(lambda x : x['data'] + 1, s, input_info={'data' : 'data'},
           output_info=(('data', {}),))
s3 = es.map(print, s2, input_info={'data' : 'data'},
            output_info=(('data',{'dtype' : 'array'}),))

event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
# generate the event streams again since generator is exhausted
#event_streams = gen_imgs([data1, data2], name="Alex", sample="FeO")
for namedocpair in event_streams:
    s.emit(namedocpair)

