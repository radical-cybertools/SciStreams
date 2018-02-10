# this is the distributed version. It assumes that the StreamDocs are handled
# as Futures
# test a XS run
import matplotlib
matplotlib.use("Agg")  # noqa
import time
import numpy as np

from collections import deque


import matplotlib.pyplot as plt
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

from SciStreams.globals import client

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
from SciStreams.interfaces.plotting_mpl import store_results_mpl


# TODO : move these out and put in stream folder
# # use reg stream mapping
# from SciStreams.streams.XS_Streams import normalize_calib_dict,\
#   add_detector_info
from SciStreams.streams.XS_Streams import make_calibration

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


# We need to normalize metadata, which is used for saving, somewhere
# in our case, it's simpler to do this in the beginning
sin = sc.Stream(stream_name="Input")
# tmp123 = sin.map(print)

stream_input = SciStreamCallback(sin.emit, remote=False)

# these are abbreviations just to make streams access easier
# this stream filters out data. only outputs data that will work in rest of
# stream
sin_primary, sout_primary, serr_primary = PrimaryFilteringStream()
sin.connect(sin_primary)
class _NUMBER_IMAGES:
    NUMBER_IMAGES=0
    def inc(self, *args, **kwargs):
        self.NUMBER_IMAGES+=1
    def __call__(self):
        return self.NUMBER_IMAGES

NUMBER_IMAGES = _NUMBER_IMAGES()
sin.sink(NUMBER_IMAGES.inc)

# sink to list for debugging
# L_primary = sout_primary.sink_to_list()


# get the attributes, clean them up and return
# new sout_primary
sin_attributes, sout_attributes = AttributeNormalizingStream()
# L_attributes = sout_attributes.sink_to_list()
sout_primary.connect(sin_attributes)

sout_primary = scs.merge(sc.zip(sout_primary,
                         scs.to_attributes(sout_attributes)))

sin_calib, sout_calib = CalibrationStream()
sout_attributes.connect(sin_calib)
# L_calib = sout_calib.sink_to_list()

# sout_calib2 = sout_calib
# sout_calib = sc.Stream()

# the PrimaryFilteringStream already split the detectors
# s_image = sc.Stream()
s_image = scs.add_attributes(sout_primary, stream_name="image")
# L_image = s_image.sink_to_list()


# TODO : fix and remove this is for pilatus300 should be in mask gen
s_mask = scs.map(generate_mask, sout_attributes)
# L_mask = s_mask.sink_to_list()


# #s_zipped =
# #L_zipped= s_zipped.sink_to_list()
s_imgmaskcalib = scs.merge(sc.zip(s_image, sout_calib, s_mask))
# L_imgmaskcalib = s_imgmaskcalib.sink_to_list()


# some small streams
def get_origin(**kwargs):
    ''' get the origin from the attributes.'''
    x = kwargs.get('beamx0', None)
    y = kwargs.get('beamy0', None)
    if x is None or y is None:
        origin = None
    else:
        origin = (y['value'], x['value'])

    return dict(origin=origin)


def get_exposure(**kwargs):
    return dict(exposure_time=kwargs.get('sample_exposure_time', None))


def get_stitch(**kwargs):
    return dict(stitchback=kwargs.get('stitchback', False))


s_origin = scs.map(get_origin, sout_attributes)

s_exposure = scs.map(get_exposure, sout_attributes)

s_stitch = scs.map(get_stitch, sout_attributes)
# name the stream for proper output

# circular average
sin_circavg, sout_circavg = CircularAverageStream()
s_imgmaskcalib.connect(sin_circavg)
# L_circavg = sout_circavg.sink_to_list()

# peak finding
sin_peakfind, sout_peakfind = PeakFindingStream()
sout_circavg.connect(sin_peakfind)

# merge with sq
sout_sqpeaks = scs.merge(sc.zip(sout_circavg, scs.select(sout_peakfind,
                                'inds_peak', 'peaksx', 'peaksy')))

# L_sqpeaks = sout_sqpeaks.sink_to_list()


def normexposure(image, exposure_time):
    return dict(image=image/exposure_time)


# image stitching
# normalize by exposure time
s_imagenorm = scs.map(normexposure, scs.merge(sc.zip(s_exposure, s_image)))
# use this for image stitch
s_imgmaskoriginstitch = scs.merge(sc.zip(s_imagenorm,
                                         s_mask,
                                         s_origin,
                                         s_stitch))


sin_stitched, sout_stitched = ImageStitchingStream(return_intermediate=True)
# NOTE : disconnected image stitching
s_imgmaskoriginstitch.connect(sin_stitched)

# L_stitched = sout_stitched.sink_to_list()


def get_shape(**kwargs):
    img = kwargs.get('image', None)
    origin = kwargs.get('origin', None)

    # this is to make a new calibration object for stitched images
    if img is None:
        raise ValueError("get_shape : img is None")

    if origin is None:
        raise ValueError("get_shape : origin is None")

    y0, x0 = origin

    return dict(origin=origin, shape=img.shape)


# TODO : only spawn new process if a condition is met
sout_stitched_attributes = scs.map(get_shape, sout_stitched)
sout_stitched_attributes = scs.merge(sc.zip(sout_attributes,
                                            sout_stitched_attributes))
s_calib_stitched = scs.map(make_calibration, sout_stitched_attributes)


# the masked image. sometimes useful to use
def maskimg(image, mask):
    return dict(image=image*mask)


s_maskedimg = scs.map(maskimg, scs.select(s_imgmaskcalib, 'image', 'mask'))
# L_maskedimg = s_maskedimg.sink_to_list()

# make qphiavg image
#
s_img_mask_origin = scs.merge(sc.zip(s_image, s_mask, s_origin))
s_qmap = scs.map(lambda calibration: dict(q_map=calibration.q_map),
                 sout_calib)
s_img_mask_origin_qmap = scs.merge(sc.zip(s_img_mask_origin, s_qmap))

sin_qphiavg, sout_qphiavg = QPHIMapStream()
s_img_mask_origin_qmap.connect(sin_qphiavg)

# L_qphiavg = sout_qphiavg.sink_to_list()

sout_sqphipeaks = scs.merge(sc.zip(sout_qphiavg, scs.select(sout_peakfind,
                                                            'inds_peak',
                                                            'peaksx',
                                                            'peaksy')))
sout_sqphipeaks = scs.select(sout_sqphipeaks, ('sqphi', 'image'), ('qs', 'y'),
                             ('phis', 'x'), ('peaksx', 'vals'))

# L_sqphipeaks = sout_sqphipeaks.sink_to_list()

sin_linecuts, sout_linecuts = LineCutStream(axis=0)
sout_sqphipeaks.connect(sin_linecuts)
# L_linecuts = sout_linecuts.sink_to_list()


sin_thumb, sout_thumb = ThumbStream(blur=2, crop=None, resize=10)
s_image.connect(sin_thumb)

sin_angularcorr, sout_angularcorr = AngularCorrelatorStream(bins=(800, 360))
#s_img_mask_origin_qmap.connect(sin_angularcorr)


# L_angularcorr = sout_angularcorr.sink_to_list()

sout_angularcorrpeaks = scs.merge(sc.zip(sout_angularcorr,
                                         scs.select(sout_peakfind,
                                                    'inds_peak',
                                                    'peaksx',
                                                    'peaksy')))
sout_angularcorrpeaks = scs.select(sout_angularcorrpeaks,
                                   ('rdeltaphiavg_n', 'image'),
                                   ('qvals', 'y'),
                                   ('phivals', 'x'),
                                   ('peaksx', 'vals'))

sin_linecuts_angularcorr, sout_linecuts_angularcorr = \
    LineCutStream(axis=0, name="angularcorr")
#sout_angularcorrpeaks.connect(sin_linecuts_angularcorr)
# L_linecuts_angularcorr = sout_linecuts_angularcorr.sink_to_list()


sin_tag, sout_tag = ImageTaggingStream()
s_maskedimg.connect(sin_tag)
# s_maskedimg.sink(lambda x : print("masked img : {}".format(x)))
# L_tag = sout_tag.sink_to_list()


# custom written Stream
# GISAXS line cuts stream
def collapse(image, mask, axis=0):
    ''' collapse the x dimension'''
    # tested a blocking call
    # import time
    # time.sleep(100)
    result = np.sum(image, axis=axis)
    result = result/np.sum(mask, axis=axis)
    return dict(linecut=result)


sin_gisaxs = sc.Stream()
# get the line cuts
sout_gisaxs_x = scs.map(collapse, sin_gisaxs, axis=1)
sout_gisaxs_x = scs.add_attributes(sout_gisaxs_x, stream_name="gisaxs-linex")
# L_gisaxs_x = sout_gisaxs_x.sink_to_list()

sout_gisaxs_y = scs.map(collapse, sin_gisaxs, axis=0)
sout_gisaxs_y = scs.add_attributes(sout_gisaxs_y, stream_name="gisaxs-liney")

sin_imgmask = scs.merge(s_image.zip(s_mask))
sin_imgmask.connect(sin_gisaxs)


def get_results(sdoc):

    # should be moved to a general .gather() stream node method
    if hasattr(sdoc["args"], "result"):
        print("gathering result back to cluster")
        sdoc['args'] = sdoc['args'].result()

    if hasattr(sdoc["kwargs"], "result"):
        print("gathering result back to cluster")
        sdoc['kwargs'] = sdoc['kwargs'].result()


# sample on how to plot to callback and file
# (must make it an event stream again first)
# set to True to enable plotting (opens many windows)
liveplots = False
# NOTE : disabled sinking
if True:

    # make event streams for some sinks
    event_stream_err_primary = scs.to_event_stream(serr_primary)
    event_stream_img = scs.to_event_stream(s_image)
    event_stream_sqphi = scs.to_event_stream(sout_qphiavg)
    event_stream_sq = scs.to_event_stream(sout_circavg)
    event_stream_peaks = scs.to_event_stream(sout_sqpeaks)
    event_stream_maskedimg = scs.to_event_stream(s_maskedimg)
    event_stream_stitched = scs.to_event_stream(sout_stitched)
    event_stream_linecuts = scs.to_event_stream(sout_linecuts)
    event_stream_thumb = scs.to_event_stream(sout_thumb)
    event_stream_angularcorr = scs.to_event_stream(sout_angularcorr)
    event_stream_linecuts_angularcorr = \
        scs.to_event_stream(sout_linecuts_angularcorr)
    event_stream_tag = scs.to_event_stream(sout_tag)
    event_stream_gisaxs_x = scs.to_event_stream(sout_gisaxs_x)
    event_stream_gisaxs_y = scs.to_event_stream(sout_gisaxs_y)

    if liveplots:
        from SciStreams.callbacks.live import LiveImage, LivePlot
        liveplot_sq = LivePlot('sqy', x='sqx', logx=True, logy=True)
        liveimage_img = LiveImage('image', cmap="inferno", tofile="image.png",
                                  norm=normalizer)
        liveimage_sqphi = LiveImage('sqphi', cmap="inferno", aspect="auto",
                                    tofile="sqphi.png", norm=normalizer)
        liveimage_maskedimg = LiveImage('image', cmap="inferno", aspect="auto",
                                        tofile="masked_image.png",
                                        norm=normalizer)

        # sample on how to make an event stream again
        # turn outputs into event streams first (for databroker
        # compatibility in the future)

        # output to plotting  callbacks
        event_stream_img.sink(scs.star(liveimage_img))
        event_stream_sqphi.sink(scs.star(liveimage_sqphi))
        event_stream_sq.sink(scs.star(liveplot_sq))
        event_stream_maskedimg.sink(scs.star(liveimage_maskedimg))

    # output to storing callbacks

    # whether plots should be done remote or not
    remote_plots=True

    plot_storage_img = scs.star(SciStreamCallback(store_results_mpl,
                                                  images=['image'],
                                                  img_norm=normalizer,
                                                  remote=remote_plots))
    plot_storage_stitch = scs.star(SciStreamCallback(store_results_mpl,
                                                     images=['image'],
                                                     img_norm=normalizer,
                                                     remote=remote_plots))
    xlbl = "$q,(\AA^{-1})$"
    ylbl = "I(q)"
    plot_storage_sq = scs.star(SciStreamCallback(store_results_mpl,
                                                 lines=[('sqx', 'sqy')],
                                                 scale='loglog', xlabel=xlbl,
                                                 ylabel=ylbl,
                                                 remote=remote_plots))
    plot_storage_sqphi = scs.star(SciStreamCallback(store_results_mpl,
                                                    images=['sqphi'],
                                                    img_norm=normalizer,
                                                    aspect='auto',
                                                    xlabel="$\phi\,$(radians)",
                                                    ylabel="$q\,$(pixel)",
                                                    remote=remote_plots))
    plot_storage_peaks = scs.star(SciStreamCallback(store_results_mpl,
                                                    lines=[dict(x='sqx',
                                                                y='sqy'),
                                                           dict(x='peaksx',
                                                                y='peaksy',
                                                                marker='o',
                                                                color='r',
                                                                linewidth=0)],
                                                    xlabel=xlbl, ylabel=ylbl,
                                                    remote=remote_plots))
    plot_storage_linecuts = \
        scs.star(SciStreamCallback(store_results_mpl,
                                   linecuts=[('linecuts_domain',  # x
                                              'linecuts',  # y
                                              'linecuts_vals')],
                                   remote=remote_plots))  # val
    plot_storage_thumb = scs.star(SciStreamCallback(store_results_mpl,
                                                    images=['thumb'],
                                                    img_norm=normalizer,
                                                    remote=remote_plots))
    plot_storage_angularcorr = \
        scs.star(SciStreamCallback(store_results_mpl,
                                   images=['rdeltaphiavg_n'],
                                   img_norm=normalizer, vmin=0, vmax=1,
                                   xlabel="$\phi\,(radians)$",
                                   ylabel="q\,(pixel)",
                                   aspect='auto',
                                   remote=remote_plots))

    plot_storage_linecuts_angularcorr = \
        scs.star(SciStreamCallback(store_results_mpl,
                                   linecuts=[('linecuts_domain',  # x
                                              'linecuts',  # y
                                              'linecuts_vals')],
                                   xlabel="$\Delta\phi$\,(radians)$",
                                   ylabel="$c(\Delta\phi)$",
                                   remote=remote_plots))  # value

    plot_storage_gisaxs = scs.star(SciStreamCallback(store_results_mpl,
                                                     lines=['linecut'],
                                                     remote=remote_plots))

    sc.sink(event_stream_img, plot_storage_img)
    sc.sink(event_stream_img,
            scs.star(SciStreamCallback(store_results_hdf5)))
    sc.sink(event_stream_stitched, plot_storage_stitch)
    sc.sink(event_stream_sq, plot_storage_sq)
    sc.sink(event_stream_sqphi, plot_storage_sqphi)
    sc.sink(event_stream_peaks, plot_storage_peaks)
    sc.sink(event_stream_peaks,
            scs.star(SciStreamCallback(store_results_hdf5)))
    #sc.sink(event_stream_linecuts, plot_storage_linecuts)
    #sc.sink(event_stream_thumb, plot_storage_thumb)
    #sc.sink(event_stream_angularcorr, plot_storage_angularcorr)
    #sc.sink(event_stream_linecuts_angularcorr,
            #plot_storage_linecuts_angularcorr)
    #sc.sink(event_stream_gisaxs_x, plot_storage_gisaxs)
    #sc.sink(event_stream_gisaxs_y, plot_storage_gisaxs)

    # save the peaks info
    #sc.sink(event_stream_peaks,
            #scs.star(SciStreamCallback(store_results_hdf5)))

    #sc.sink(event_stream_img,
            #scs.star(SciStreamCallback(store_results_hdf5)))

    #sc.sink(event_stream_tag,
    #       scs.star(SciStreamCallback(store_results_xml)))
    #sc.sink(event_stream_tag, print)

    sc.sink(event_stream_err_primary,
            scs.star(SciStreamCallback(store_results_xml,
                                       stream_name="error")))
    #sc.sink(event_stream_tag,
            #scs.star(SciStreamCallback(store_results_hdf5)))
    # scs.map(print, event_stream_img)

def fill_events(doc, descriptor, db=None):
    ''' fill events in place'''
    # the subset of self.fields that are (1) in the doc and (2) unfilled
    # print("Filled events")
    # print(doc['filled'])
    if 'filled' in doc:
        non_filled = [key for key, val in doc['filled'].items() if not val]
        # print("non filled : {}".format(non_filled))
    else:
        non_filled = []

    if len(non_filled) > 0:
        db.fill_event(event=doc, inplace=True)

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


def future_collector(queue1, donequeue, delay=.1):
    ''' Collect futures. Just call result()'''
    while True:
        print("Checking done status")
        if len(donequeue) == 0  and len(queue1) == 0:
            print("Done, exiting...")
            break
        print("Checking for a future...")
        if len(queue1) > 0:
            # pop first element
            next_item = queue1.popleft()
            print(next_item)
            if next_item.done() is False:
                # add to end
                queue1.append(next_item)
                time.sleep(delay)
        else:
            time.sleep(delay)


def queue_monitor(queue1, donequeue, output_file):
    print("##\nQueue monitor started\n")
    t0 = time.time()
    with open(output_file, "w") as f:
        while len(donequeue) > 0 or len(queue1) > 0:
            print("Queue not done")
            t1 = time.time()
            msg = "{:4.2f}\t{}\t{}\n".format(t1-t0, len(queue1), int(len(donequeue)==0))
            f.write(msg)
            time.sleep(.1)
    print("####\nQueue done!!!")

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
    from SciStreams.interfaces.databroker.databases import databases

    #start the future collector
    from threading import Thread
    from SciStreams.globals import futures_cache_sinks
    donequeue = deque()
    donequeue.append(1)

    thread = Thread(target=future_collector, args=(futures_cache_sinks,donequeue))
    thread.start()

    thread_mon = Thread(target=queue_monitor, args=(futures_cache_sinks, donequeue, queue_monitor_filename))
    thread_mon.start()

    cmsdb = databases['cms:data']

    kwargs = dict()
    if stop_time is not None:
        kwargs['stop_time'] = stop_time
    if start_time is not None:
        kwargs['start_time'] = start_time

    from uuid import uuid4

    def stream_gen(hdrs):
        ''' Intercept FileNotFoundError from file handler.
            Issue a stop if some error is raised
        '''
        current_start = None
        for hdr in hdrs:
            gen = hdr.documents(fill=True)
            try:
                for nds in gen:
                    if nds[0] == 'start':
                        current_start = nds[1]['uid']
                    yield nds
            # except FileNotFoundError:
            except Exception:
                # don't yield event, just give a stop
                yield ('stop', {'uid': str(uuid4()),
                                'run_start': current_start})
    if uids is not None:
        print("Starting a run only on selected uids")
        hdrs = cmsdb[uids]
        loop_forever = False

    while True:
        hdrs = cmsdb(**kwargs)
        stream = stream_gen(hdrs)

        # stream converter
        stream_buffer = BufferStream()

        # some loop over stream
        # TODO look for FileNotFoundError in nds iteration
        # make sure it's an iterator
        stream = iter(stream)
        last_start = None
        while True:
            try:
                # add a waiting loop
                wait_on_client()
                nds = stream_buffer(next(stream))
                if nds[0] == 'start':
                    last_start = nds[1][2]
                print("iterating : {}".format(nds[0]))
                stream_input(*nds)
                plt.pause(.1)
                if maxrun is not None and NUMBER_IMAGES() > maxrun:
                    print("Computed {} images and maxrun is {}".format(NUMBER_IMAGES(), maxrun))
                    print("Terminating...")
                    break
                print("\n\n\n\n####\n\nNumber of images sent : {}".format(NUMBER_IMAGES()))
                print("\n\n\n\n####\n\nNumber of Futures: {}".format(len(futures_cache_sinks)))
            except StopIteration:
                break
            except FileNotFoundError:
                continue
        if not loop_forever or maxrun is not None:
            print("Exiting loop")
            break

        # get the latest time, add 1 second to not overlap it
        if last_start is not None:
            last_time = last_start['time']
        else:
            last_time = time.time()
        t1 = time.localtime(last_time)
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", t1)
        kwargs['start_time'] = start_time
        # remove stop_time after first iteration
        if 'stop_time' in kwargs:
            kwargs.pop('stop_time')
        # at the end, update time stamp for latest time
        msg = "Reached end, waiting "
        msg += "{} sec for more data...".format(poll_interval)
        print(msg)
        time.sleep(poll_interval)

    donequeue.pop()
    print("Final Count : {} images analyzed".format(NUMBER_IMAGES()))
    print("Now waiting for final results to finish")
    while len(futures_cache_sinks) > 0:
        time.sleep(1)
        print("Not done, waiting...")

    #thread.join()



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
