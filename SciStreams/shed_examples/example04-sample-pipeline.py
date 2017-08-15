# try reading from databroker
import numpy as np
import time
from uuid import uuid4
import streams.core as sc
import shed.event_streams as es

# the database for stream
from SciStreams import cddb

det_key = 'pilatus2M_image'

def addmydata(x):
    data = x[det_key]
    return data + 1

# TODO : more to an object that does this
# should issue an event and generate provenance
def add_stitchback(x, stitchback=True):
    if x[0] == 'start':
        startdoc = x[1]
        if 'stitchback' not in startdoc:
            startdoc['stitchback'] = stitchback

    return x

# TODO : move to an object that does this
def attributes_to_events(x):
    # move events to attributes
    global _attrs_prevstart
    name = x[0]
    if name =='start':
        startdoc = x[1]
        _attrs_prevstart = startdoc
    elif name == 'descriptor':
        # issue new descriptor doc
        descdoc = {}
    elif name == 'event':
        eventdoc = x[1]
    return x


# bookkeeping what we use at CMS
keywords = {
    'detector_SAXS_name' : 'detector name',
}

s = sc.Stream()
# this time, we pass the stream to event_stream's
# map method
# TODO : use an event stream operator
# Add attribute?
s2 = sc.map(add_stitchback, s)
s3 = es.Eventify(s2, 'sample_name', 'time', output_info=[('name', {})])

L = list()
# map to regular stream
s4 = sc.map(L.append, s3)


# create the event stream
hdr = cddb['be27420d-de59-46ad-bfba-fdf4804ab616']
event_stream = cddb.restream(hdr, fill=True)

for namedocpair in event_stream:
    s.emit(namedocpair)


for item in L:
    print(item)
