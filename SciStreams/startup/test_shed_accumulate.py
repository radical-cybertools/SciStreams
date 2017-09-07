import shed.event_streams as es
import numpy as np
from shed.utils import to_event_model


# The data 
data1 = np.random.random((100, 100))
data2 = np.random.random((100, 100))

event_streams = to_event_model([data1, data2],
                               output_info=[("data_from_stream", {"dtype"
                                                                  :"array"})])

# generate the name doc pairs



# just test 
def myaccumulator(data):
    # here 'state' is the state key
    # and 'data_in' is the data key
    # just add them together for now
    state = data['state']
    mydata = data['data_in']
    newstate = state + mydata
    return dict(out_of_accum=newstate)


# testing out the new accumulator
s = es.Stream()
# {to : (from, #)}
# or
# {to : from}
input_info = {'data_in' : 'data_from_stream'}
output_info = [('out_of_accum', {'dtype' : 'array', 'source' : 'test'})]
s2 = es.accumulate(myaccumulator, s, input_info=input_info,
                   output_info=output_info,
                   state_key='state')
L = s2.sink_to_list()



for nds in event_streams:
    s.emit(nds)

print(L)

# need an event level sliding window
