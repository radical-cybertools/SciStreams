# this is the distributed version. it assumes that the streamdocs are handled
# as futures
# test a XS run
#import matplotlib
#matplotlib.use("Agg")  # noqa
import time
import numpy as np
print('importing stress test')

from collections import deque


#import matplotlib.pyplot as plt
# plt.ion()  # noqa

# databroker
# from databroker.assets.handlers import AreaDetectorTiffHandler

# if using dask async stuff will need this again
# from tornado.ioloop import IOLoop
# from tornado import gen

# from functools import partial

# from distributed import sync

from SciStreams.callbacks import SciStreamCallback

from SciStreams.detectors.mask_generators import generate_mask

# from SciStreams.core.StreamDoc import StreamDoc
# StreamDoc to event stream
# from SciStreams.core.StreamDoc import to_event_stream
# import SciStreams.core.StreamDoc as sd

from SciStreams.config import client

# the differen streams libs
import streamz.core as sc
import SciStreams.core.scistreams as scs


# import the different functions that make streams
from SciStreams.streams.XS_Streams import PrimaryFilteringStream
from SciStreams.streams.XS_Streams import AttributeNormalizingStream
from SciStreams.streams.XS_Streams import CalibrationStream
from SciStreams.streams.XS_Streams import CircularAverageStream
from SciStreams.streams.XS_Streams import PeakFindingStream
from SciStreams.streams.XS_Streams import QPHIMapStream
from SciStreams.streams.XS_Streams import ImageStitchingStream
from SciStreams.streams.XS_Streams import LineCutStream
from SciStreams.streams.XS_Streams import ThumbStream
from SciStreams.streams.XS_Streams import AngularCorrelatorStream
from SciStreams.streams.XS_Streams import ImageTaggingStream

# useful image normalization tool for plotting
from SciStreams.tools.image import normalizer

# interfaces imports
from SciStreams.interfaces.xml.xml import store_results_xml
from SciStreams.interfaces.hdf5 import store_results_hdf5
#from SciStreams.interfaces.plotting_mpl import store_results_mpl


# TODO : move these out and put in stream folder
# # use reg stream mapping
# from SciStreams.streams.XS_Streams import normalize_calib_dict,\
#   add_detector_info
from SciStreams.streams.XS_Streams import make_calibration

print('imported libs')
# from SciStreams.callbacks.live.core import LivePlot_Custom

# Limit pressure by stopping submitting jobs after a max has been reached
MAX_PROCESSING = 10000


def wait_on_client():
    ''' Wait on client to limit pressure.
        Depends on globals client and MAX_PROCESSING.'''
    while True:
        if not hasattr(client, 'processing'):
            return True
        free = False
        for key, val in client.processing().items():
            if len(val) < MAX_PROCESSING:
                free = True
        if free:
            break
        else:
            print("waiting on client (60s). Currently backed up...")
            time.sleep(60)
    return True


print('defining streams')
# We need to normalize metadata, which is used for saving, somewhere
# in our case, it's simpler to do this in the beginning
sin = sc.Stream(stream_name="Input")
sin.sink(print)
print('part1')
# tmp123 = sin.map(print)
import numpy as np
def test_func(*args, **kwargs):
    arr = np.ones((10000, 10000))
    return dict(arr=arr)
print('part2')

from dask import delayed
print('part3')
def submittoclient(x):
    print("Submitting to client : {}".format(client))
    return client.submit(test_func, x)

sout = sin.map(submittoclient)
sout.sink(print)
print('part4')
#sout2 = sout.map(lambda sdoc: sdoc.result()['arr'])

print('part5')
futures_cache = deque()
sout.sink(lambda x : futures_cache.append(x))

print('done defining streams')




# sample on how to plot to callback and file
# (must make it an event stream again first)
# set to True to enable plotting (opens many windows)


class BufferStream:
    ''' class mimicks callbacks, upgrades stream from a 'start', doc instance
        to a more complex ('start', (None, start_uid, doc)) instance

        Experimenting with distributed computing. This seems to work better.
        Allows me to construct tree of docs before computing the delayed docs.

        This unfortunately can't be distributed.
    '''
    def __init__(self, db=None):
        self.start_docs = dict()
        self.descriptor_docs = dict()
        self.db = db

    def __call__(self, ndpair):
        name, doc = ndpair
        parent_uid, self_uid = None, None
        if name == 'start':
            parent_uid, self_uid = None, doc['uid']
            # add the start that came through
            self.start_docs[self_uid] = parent_uid
        elif name == 'descriptor':
            parent_uid, self_uid = doc['run_start'], doc['uid']
            # now add descriptor
            self.descriptor_docs[self_uid] = parent_uid
        elif name == 'event':
            parent_uid, self_uid = doc['descriptor'], doc['uid']
            # fill events if not filled
            descriptor = self.descriptor_docs[doc['descriptor']]
            # print("before filled events : {}".format(doc))
            fill_events(doc, descriptor, db=self.db)
            # print("filled events : {}".format(doc))
        elif name == 'stop':
            # if the stop is strange, just use it to clear everything
            if 'run_start' not in doc or 'uid' not in doc:
                self.clear_all()
            else:
                parent_uid, self_uid = doc['run_start'], doc['uid']
                # clean up buffers
                self.clear_start(parent_uid)

        return name, (parent_uid, self_uid, doc)

    def clear_all(self):
        self.start_docs = dict()
        self.descriptors = dict()

    def clear_start(self, start_uid):
        # clear the start
        self.start_docs.pop(start_uid)

        # now clear the descriptors
        # first find them
        desc_uids = list()
        for desc_uid, parent_uid in self.descriptor_docs.items():
            if parent_uid == start_uid:
                desc_uids.append(desc_uid)
        # now pop them
        for desc_uid in desc_uids:
            self.descriptor_docs.pop(desc_uid)


def future_collector(queue1, queuedone, errorqueue, delay=.1):
    ''' Collect futures. Just call result()'''
    print("Started the future collector")
    while True:
        #print("Checking done status")
        if queuedone() == 1  and len(queue1) == 0:
            print("Done, exiting...")
            break
        #print("Checking for a future...")
        if len(queue1) > 0:
            # pop first element
            next_item = queue1.popleft()
            #print(next_item)
            if next_item.done() is False:
                # add to end
                if next_item.status == 'pending':
                    print("{} not done, popping back".format(next_item))
                    queue1.append(next_item)
                    time.sleep(delay)
                elif next_item.status == 'error':
                    print("There was an error with this computation.")
                    print("Putting to error queue: {}".format(next_item))
                    next_item.cancel()
                    error_queue.append(next_item.key)
                    del next_item
                else:
                    print("Unknown status: {}".format(next_item.status))
                    print("Putting to error queue: {}".format(next_item))
                    next_item.cancel()
                    error_queue.append(next_item.key)
                    del next_item
            else:
                # release the result first just in case
                # TODO : put in some deque to save for later
                next_item.cancel()
        else:
            time.sleep(delay)
    print("Finished")


def queue_monitor(queue, queuedone, output_file):
    print("##\nQueue monitor started\n")
    t0 = time.time()
    with open(output_file, "w") as f:
        while queuedone() == 0 or len(queue1) > 0:
            #print("Queue not done")
            t1 = time.time()
            msg = "{:4.2f}\t{}\t{}\n".format(t1-t0, len(queue1), queuedone())
            f.write(msg)
            time.sleep(.1)
    print("####\nQueue done!!!")

def val_monitor(val1, queuedone, output_file):
    '''
        monitor a value:
            val1 : callable that returns a value
            queuedone: callable returns result 0 if not done 1 if done
            output_file : the output file

        note : this is very messy. need to clean up with time...
    '''
    print("##\nVal monitor started\n")
    t0 = time.time()
    with open(output_file, "w") as f:
        while queuedone() != 1:
            #print("Queue not done")
            t1 = time.time()
            msg = "{:4.2f}\t{}\n".format(t1-t0, val1(), queuedone())
            f.write(msg)
            time.sleep(.1)
    print("####\nQueue done!!!")


print("imported stress test")
def start_run(start_time=None, stop_time=None, uids=None, loop_forever=True,
              poll_interval=60, maxrun=None, queue_monitor_filename="out.txt"):
    ''' Start running the streaming pipeline.

        start_time : str
            the start time for the run

        stop_time : str, optional
            the stop time of the initial search

        uids : str or list of str, optional
            if set, run only on these uids quickly

        loop_forever : bool, optional
            If true, loop forever

        poll_interval : int, optional
            poll interval (if loop is True)
            This is the interval to wait between checking for new data

        maxrun : int, optional
            DO NOT USE (for debugging only)
            specify max number of documents to run
            ensures that pipeline stops after finite time
    '''
    # patchy way to get stream for now, need to fix later
    for i in range(1000):
        print("emitting value")
        sin.emit(i)
        time.sleep(1)
        # every 10 clear
        if i % 10 == 0:
            print("clearing futures")
            while len(futures_cache):
                futures_cache.pop()

    print("Sleeping an hour")
    time.sleep(3600)




def start_callback():
    from bluesky.callbacks.zmq import RemoteDispatcher
    from SciStreams.config import config as configd

    from SciStreams.interfaces.databroker.databases import databases
    cmsdb = databases['cms:data']
    # db needed to fill events
    stream_buffer = BufferStream(db=cmsdb)
    def callback(*nds):
        nds = stream_buffer(nds)
        stream_input(*nds)
    # get the dispatcher port for bluesky zeromq process
    ipstring = "localhost:{:4d}".format(configd['bluesky']['port'])
    d = RemoteDispatcher(ipstring)
    d.subscribe(callback)
    #d.subscribe(print)

    # when done subscribing things and ready to use:
    d.start()  # runs event loop forever

#sin.visualize()
