# test a XS run
import time
from time import sleep
import os
import numpy as np
import matplotlib
#matplotlib.use("Agg")  # noqa
# from dask import delayed, compute
from collections import deque
import dask
dask.set_options(delayed_pure=True)

import matplotlib.pyplot as plt
plt.ion()

# CUSTOM USER GLOBALS (to be moved to yml file)
#MASK_DIR = "~/research/projects/SciAnalysis-data/masks"
#MASK_DIR = os.path.expanduser(MASK_DIR)
#BLEMISH_pilatus2M = MASK_DIR + "/pilatus2M_image/blemish_pilatus2M_Sept2017.tif"
#BLEMISH_pilatus300 = MASK_DIR + "/pilatus300_image/pilatus300_mask_main.png"
#MASK_NAME = "/home/lhermitte/research/projects/SciStreams/SciStreams/mask.npz.npy"

#BLEMISH_FILENAMES = dict(
        #pilatus300_image=BLEMISH_pilatus300,
        #pilatus2M_image=BLEMISH_pilatus2M,
        #)

from functools import partial

# SciStreams imports
# this one does a bit of setup upon import, necessary
# from SciStreams.globals import client
from SciStreams.config import config, mask_config

# interfaces for saving to databroker
# NEED TO TURN INTO BLUESKY-LIKE CALLBACKS
#from SciStreams.interfaces.plotting_mpl import plotting_mpl as iplotting
#from SciStreams.interfaces.databroker import databroker as idb
#from SciStreams.interfaces.file import file as ifile
#from SciStreams.interfaces.xml import xml as ixml
# from SciStreams.interfaces.detectors import detectors2D
# Streams include stuff

# Analyses
# WILL NEED TO BE IMPLEMENTED LATER
from SciStreams.data.Mask import \
        MasterMask, MaskGenerator

# if using dask async stuff will need this again
from tornado.ioloop import IOLoop
from tornado import gen

from SciStreams.callbacks import CallbackBase
# simple callback to feed data to stream
class FeedToStream(CallbackBase):
    def __init__(*args, stream, **kwargs):
        self.stream = stream
        super(FeedToStream, self).__init__(*args, **kwargs)

    def start(self, doc):
        self.stream.emit(("start", doc))

    def stop(self, doc):
        self.stream.emit(("stop", doc))

    def event(self, doc):
        self.stream.emit(("event", doc))

    def descriptor(self, doc):
        self.stream.emit(("descriptor", doc))

class LivePlot_Custom(CallbackBase):
    def start(self, doc):
        self.fignum = plt.figure().number

    def event(self, doc):
        img = doc[0]['data']['image']

        attrs = doc[1]['data']
        xkey = 'beamx0'
        ykey = 'beamy0'
        if xkey in attrs:
            x0 = attrs[xkey]['value']
            y0 = attrs[ykey]['value']
        else:
            x0, y0 = None, None

        plt.figure(self.fignum);
        plt.clf();
        plt.imshow(img);plt.clim(0,100)
        if x0 is not None and y0 is not None:
            plt.plot(x0, y0, 'ro')




from SciStreams.detectors import detectors2D
from SciStreams.detectors.mask_generators import generate_mask


def safelog10(img):
    img_out = np.zeros_like(img)
    w = np.where(img > 0)
    img_out[w] = np.log10(img[w])
    return img_out


# sample custom written function
def PCA_fit(data, n_components=10):
    ''' Run principle component analysis on data.
        n_components : num components (default 10)
    '''
    # first reshape data if needed
    if data.ndim > 2:
        datashape = data.shape[1:]
        data = data.reshape((data.shape[0], -1))

    from sklearn.decomposition import PCA
    pca = PCA(n_components=n_components)
    pca.fit(data)
    components = pca.components_.copy()
    components = components.reshape((n_components, *datashape))
    return dict(components=components)


def isSAXS(sdoc):
    ''' return true only if a SAXS expt.'''
    attr = sdoc['attributes']
    if 'experiment_type' in attr:
        # print("experiment type : {}".format(attr['experiment_type']))
        expttype = attr['experiment_type']
        if expttype == 'SAXS' or expttype == 'TSAXS':
            return True
    return False




# use map to take no input info 
#
def pick_arrays(**kwargs):
    ''' Only pass through array events, ignore rest.
    '''
    new_kwargs = dict()
    for key, val in kwargs.items():
        if hasattr(val, 'ndim') and val.ndim > 0:
            new_kwargs[key] = val
    return new_kwargs



import shed.event_streams as es
import streamz.core as sc


globaldict = dict()


def streamdoc_viewer(sdoc):
    print("StreamDoc : {}".format(sdoc['uid']))
    nargs = len(sdoc.args)
    nkwargs = len(sdoc.kwargs)
    kwargs_keys = list(sdoc.kwargs.keys())
    print("number of args: {}".format(nargs))
    print("kwargs keys {}".format(kwargs_keys))


def document_viewer(nds):
    docname = nds[0]
    doc = nds[1]
    #print("Current Document : {}".format(docname))
    if docname == "start":
        md = doc
        print("Start document. Uid : {}".format(doc['uid']))
        print("Metadata keys : {}".format(md.keys()))
    elif docname == "descriptor":
        data_keys = doc['data_keys']
        print("Descriptor. Data keys: {}".format(data_keys))
    elif docname == "event":
        data_keys = doc['data'].keys()
        print("Event. Data keys : {}".format(data_keys))
    elif docname == "stop":
        start_uid = doc['run_start']
        print("Stop document. Run start uid: {}".format(start_uid))

    print("End document description of {}".format(docname))



# This is the main stream of data
# Stream setup, datbroker data comes here (a string uid)
#sin = es.EventStream()


import streamz.core as sc
import SciStreams.core.StreamDoc as sd
import SciStreams.core.scistreams as scs
from SciStreams.core.StreamDoc import StreamDoc

class SciStreamCallback(CallbackBase):
    # dictionary of start documents
    def __init__(self, sin, *args, **kwargs):
        self.stream_input = sin
        self.start_docs = dict()
        self.descriptors = dict()
        super(SciStreamCallback, self).__init__(*args, **kwargs)

    def start(self, doc):
        self.start_docs[doc['uid']] = doc

    def descriptor(self, doc):
        start_uid = doc['run_start']
        start_docs = self.start_docs
        if start_uid not in start_docs:
            msg = "Missing start for descriptor"
            msg += "\nDescriptor uid : {}".format(doc['uid'])
            msg += "\nStart uid: {}".format(start_uid)
            raise Exception(msg)
        self.descriptors[doc['uid']] = doc

    def event(self, doc):
        descriptor_uid = doc['descriptor']
        if descriptor_uid not in self.descriptors:
            msg = "Missing descriptor for event"
            msg += "\nEvent uid : {}".format(doc['uid'])
            msg += "\nDescriptor uid: {}".format(descriptor_uid)
            raise Exception(msg)

        descriptor = self.descriptors[descriptor_uid]
        start_uid = descriptor['run_start']
        start_doc = self.start_docs[start_uid]

        data = doc['data']

        # now make data
        event = StreamDoc()
        event.add(attributes=start_doc)
        # no args since each element in a doc is named
        event.add(kwargs=data)
        checkpoint = dict(parent_uids=[start_uid])
        provenance = dict(name="SciStreamCallback")
        event.add(checkpoint=checkpoint)
        event.add(provenance=provenance)
        self.stream_input.emit(event)

    def stop(self, doc):
        ''' Stop is where the garbage collection happens.'''
        start_uid = doc['run_start']
        # cleanup the start with start_uid
        self.cleanup_start(start_uid)

    def cleanup_start(self, start_uid):
        if start_uid not in self.start_docs:
            msg = "Error missing start for stop"
            raise Exception(msg)
        self.start_docs.pop(start_uid)
        desc_uids = list()
        for desc_uid, desc in self.descriptors.items():
            if desc['run_start'] == start_uid:
                desc_uids.append(desc_uid)

        for desc_uid in desc_uids:
            self.cleanup_descriptor(desc_uid)

    def cleanup_descriptor(self, desc_uid):
        if desc_uid in self.descriptors:
            self.descriptors.pop(desc_uid)






def filter_detectors(sdoc):
    #print(sdoc)
    dets = ['pilatus2M_image', 'pilatus300_image']
    data_keys = list(sdoc.kwargs.keys())
    print(data_keys)
    found_det = False
    for det in dets:
        if det in data_keys:
            found_det=True

    if found_det:
        print("found dets")
    return found_det

sin = sc.Stream()
stream_input = SciStreamCallback(sin)
#sin_primary = sc.filter(lambda x : x[0]['name'] == 'primary', sin,
                        #full_event=True, document_name='descriptor',
                        #keep_start=True)


# TODO : having to set full_event True seems awkward, maybe PR a change?
#sin_primary = es.filter(filter_detectors, sin_primary,
                        #full_event=True, document_name='descriptor')

sin_primary = sc.filter(filter_detectors, sin)


# use reg stream mapping
sin_primary.map(streamdoc_viewer)

# TODO : run asynchronously?

def get_detector_key_start(**md):
    # a cludge to get detector name
    md['detector_key'] = md['detectors'][0] + "_image"
    return md
###
# prepare the attribues in some normalized form, add data and 
# also make assumption that there is one detector here
from SciStreams.streams.XS_Streams import normalize_calib_dict,\
        add_detector_info, make_calibration
# First split it off into attributes, but reading the metadata (Eventify)
# and passing to a calibration object
# push attributes into actual event data before we lose them
s_attributes = scs.get_attributes(sin_primary)
#s_attributes = es.Eventify(sin_primary)
# set the detector name from det key in start (adding metadata)
#s_attributes = es.map(get_detector_key_start, s_attributes)

#s_attributes = es.map(normalize_calib_dict, s_attributes, keymap_name='cms')
#s_attributes = es.map(add_detector_info, s_attributes)

from SciStreams.detectors.mask_generators import generate_mask




# TODO : The detector key should be taken from descriptor
# TODO : This assumes events for images are length 1. If more
# than 1, need to repeat the s_calib stream (which is just 1 event)
# first pick the detector, just the first one for now
# TODO : add splits of stream to compute for multiple detectors
# next make a calibration object
s_calib = scs.map(make_calibration, s_attributes)
s_calib.map(streamdoc_viewer)

# TODO : make a "grab first detector" argument (looks for first data element
# that's an array and returns the name)
#
## now try caching, the object has its own builtin hashing that dask understands
def _generate_qxyz_maps(calibration):
    calibration.generate_maps()
    return calibration
from SciStreams.globals import client
import SciStreams.globals as streams_globals
s_calib_obj = scs.map(lambda calibration:
                          client.submit(_generate_qxyz_maps, calibration),
                          s_calib)

scs.map(lambda calibration : streams_globals.futures_cache.append(calibration),
        s_calib_obj)
s_calib_obj = scs.map(lambda x: client.gather(x), s_calib_obj)
# END Calibration

## assume events coming in are a stream
s_data = sin_primary
#s_image.map(print)
#s_event = es.Eventify(s_event, None)
# TODO : synchronize this with start doc (maybe choose only the data key
# corresponding to metadata)
s_data = scs.map(pick_arrays, s_data)
def grab_first_det(**kwargs):
    ''' take first det and make it image key
        cludge for now
    '''
    key = list(kwargs.keys())[0]
    return dict(image=kwargs[key], detector_key=key)

def grab_first_det_key(**kwargs):
    ''' take first det and make it image key
        cludge for now
    '''
    key = list(kwargs.keys())[0]
    return dict(detector_key=key)

s_image = scs.map(grab_first_det, s_data)
# TODO : synchronize with start doc
s_detector_key = scs.map(grab_first_det_key, s_data)
#s_out = sc.map(print, s_event)

#s_attributes.map(print)
s_mask = scs.map(generate_mask, s_attributes)

s_imgmaskcalib = sc.zip(s_image, s_calib_obj, s_mask)
s_imgcalibmask = scs.merge(s_imgmaskcalib)

def get_stitch(**kwargs):
    return dict(stitchback=kwargs.get('stitchback', False))

def get_exposure(**kwargs):
    return dict(exposure=kwargs.get('sample_exposure_time', None))

def get_origin(**kwargs):
    ''' get the origin from the attributes.'''
    x = kwargs.get('detector_SAXS_x0_pix', None)
    y = kwargs.get('detector_SAXS_y0_pix', None)
    if x is None or y is None:
        origin = None
    else:
        origin = (y,x)

    return dict(origin=origin)

s_origin = scs.map(get_origin, s_attributes)

s_exposure = scs.map(get_exposure, s_attributes)

# move stitch to an event by grabbing from attributes
s_stitch = scs.map(get_stitch, s_attributes)

# merge streams to create zipped stream
from SciStreams.streams.XS_Streams import circavg_from_calibration
#def circavg_from_calibration(image, calibration, mask=None, bins=None):

s_circavg = scs.map(circavg_from_calibration, s_imgmaskcalib)


def pack_args(image, mask, origin, stitchback):
    ''' need to make into a tuple.'''
    return (image, mask, origin, stitchback)

def xystitch_result_shed(curstate):
    img_acc, mask_acc, origin_acc, stitchback_acc = curstate
    return xystitch_result(img_acc=img_acc, mask_acc=mask_acc,
                           origin_acc=origin_acc,
                           stitchback_acc=stitchback_acc)


#from SciStreams.processing.stitching import xystitch_accumulate, xystitch_result
#s_imgmaskoriginstitch = scs.merge(sc.zip(s_image, s_mask, s_origin, s_stitch))
#
#s_stitched = scs.map(pack_args, s_imgmaskoriginstitch)
#s_stitched = scs.accumulate((xystitch_accumulate), s_stitched)
#
#s_stitched = scs.map(xystitch_result_shed, s_stitched)
#
#def grab_stitched_image(image, mask, origin, stitchback):
#    return image
#
#s_stitched_image = es.map(grab_stitched_image, s_stitched,
#                            output_info=[('image', {'filled' : True,
#                                                #'shape' : (1679, 1475)})])
#                                                'shape' : (1679, 1475)})])
#
#from SciStreams.processing.qphiavg import qphiavg
#
#s_img_mask_origin = es.map(lambda **kwargs : kwargs, es.zip(s_image, s_mask, s_origin),
#                           input_info={'image' : ('image', 0),
#                                       'mask' : ('mask', 1),
#                                       'origin' : ('origin', 2)
#                                       }
#                           )
#
#
#####TEMPORARY
#L_img_mask = s_img_mask_origin.sink_to_list()
#
### ######
#
#s_qphiavg = es.map(qphiavg, s_img_mask_origin, bins=(800,360))
#def grab_sqphi(**kwargs):
#    return kwargs.get('sqphi', None)
#s_sqphi = es.map(grab_sqphi, s_qphiavg,
#        output_info=[('sqphi', {'filled' : True, 'shape' : ()})])
#L = s_qphiavg.sink_to_list()
#
#from SciStreams.processing.angularcorr import angular_corr
#s_angularcorr = es.map(angular_corr, s_img_mask_origin, bins=(800, 360))
#L_angcorr = s_angularcorr.sink_to_list()
#
#
#
## attach to callbacks here
## eventually re-write data outputs in terms of callbacks
## some callbacks are just bluesky imports. Also all callbacks inherit bluesky
## templates. Maybe it may be better to remove bluesky dep in future?
#from SciStreams.callbacks.live import LiveImage, LivePlot
#
## TODO : use kde's for normalization
#def image_norm(image, low=.01, high=.99):
#    ''' this is just threshold normalization
#        Assumes positive valued images
#    '''
#    img = image
#    data = img[~np.isnan(img)*~np.isinf(img)]
#    nobins = min(65535, max(1, int(np.max(data))))
#    hh, bin_edges = np.histogram(data, bins=nobins)
#    cumcnts = np.cumsum(hh)
#    hhsum = np.sum(hh)
#
#
#    # only threshold if there are counts
#    if hhsum > 0:
#        cumcnts = cumcnts/hhsum
#        wlow = np.where(cumcnts > low)[0]
#        if len(wlow) > 0:
#            wlow = wlow[0]
#        else:
#            wlow = 0
#        whigh = np.where(cumcnts < high)[0]
#        if len(whigh) > 1:
#            whigh = whigh[-1]
#        else:
#            whigh = 1
#        img = ((img>wlow) < whigh)*img
#        img[np.isnan(img)+np.isinf(img)] = 0
#
#    return image
#
#def reduce_img(**kwargs):
#    ''' reduce the image to 256x256 for the machine learning inference.'''
#    from skimage.transform import downscale_local_mean
#    img = kwargs['image']
#    desired_dims = 256, 256
#    cts_img = np.ones_like(img)
#
#    # just clip the edges
#    # later, could keep edge information by averaging
#    edgey, edgex = img.shape[0] - img.shape[0]%desired_dims[0],\
#            img.shape[1] - img.shape[1]%desired_dims[1]
#    facy, facx = img.shape[0]//desired_dims[0],\
#            img.shape[1]//desired_dims[1]
#
#    # this subimg should downscale to 256
#    subimg = img[:edgey, :edgex]
#    cts_img = cts_img[:edgey, :edgex]
#    down_img = downscale_local_mean(subimg, (facy, facx), cval=0)
#    cts = downscale_local_mean(cts_img, (facy, facx), cval=0)
#
#    w = np.where(cts != 0)
#    down_img[w] /= cts[w]
#    w = np.where(cts == 0)
#    down_img[w] = 0
#
#    return dict(image=down_img)
#
#
##liveimage_stitch = LiveImage('image', cmap="inferno")
##es.map(image_norm, s_stitched_image, output_info=[('image', {})]).sink(es.star(liveimage_stitch))
##
##liveimage_sqphi = LiveImage('sqphi', aspect='auto')
##es.map(image_norm, s_sqphi, input_info={'image' : 'sqphi'},
##        output_info=[('sqphi', {})]).sink(es.star(liveimage_sqphi))
##
##liveimage_angularcorr = LiveImage('rdeltaphiavg_n', aspect='auto')
##es.map(image_norm, s_angularcorr, input_info={'image' : 'rdeltaphiavg_n'},
##        output_info=[('rdeltaphiavg_n', {})]).sink(es.star(liveimage_sqphi))
##
##liveimage_sqplot = LivePlot('sqy', x='sqx')
##s_circavg.sink(es.star(liveimage_sqplot))
##
##liveimage_withcen = LivePlot_Custom()
##es.zip(s_stitched_image, s_attributes).sink(es.star(liveimage_withcen))
#
#
#s_stitch_attrs = es.zip(s_stitch, s_attributes)
## save data to file here
#from SciStreams.callbacks.saving_mpl.core import store_results
#es.map(store_results, s_stitch_attrs)




#figure(2);clf()
#es.map(lambda mask: dict(foo=imshow(mask)), s_mask)

# next start working on result
#s_event = s_event\
        #.map(add_attributes, stream_name="InputStream")
#s_event = s_event\
        #.map(set_detector_name, detector_name='pilatus300')

from databroker.assets.handlers import AreaDetectorTiffHandler
class TiffHandler(AreaDetectorTiffHandler):
    def __call__(self, point_number):
        # if File not Found, return None
        try:
            res = AreaDetectorTiffHandler.__call__(self, point_number)
        except FileNotFoundError:
            res = None
        return res

if True:
    # patchy way to get stream for now, need to fix later
    from SciStreams.interfaces.databroker.databases import databases

    cmsdb = databases['cms:data']
    # register a handler that ignores file not found
    cmsdb.reg.register_handler("AD_TIFF", TiffHandler, overwrite=True)

    #hdrs = cmsdb(start_time="2017-07-13", stop_time="2017-07-14")# 16:00")
    hdrs = cmsdb(start_time="2017-09-13", stop_time="2017-09-14 16:00")
    stream = cmsdb.restream(hdrs, fill=True)

elif True:
    # simulate the data
    from shed.utils import to_event_model
    Ndata = 10
    shape = 619, 487
    data = np.random.random((Ndata, *shape))
    output_info = (('data', {'dtype' : 'array'}), )
    # some metadata
    md = {'sample_name' : "AgBH"}
    # some typical CMS calibration stuff
    md_calib = {
            'calibration_energy_keV' : 13.5,
            'calibration_wavelength_A' : .9184,
            'detector_SAXS_x0_pix' : 100,
            'detector_SAXS_y0_pix' : 100,
            'detector_SAXS_distance_m' : 5,
            'detectors' : ['pilatus300'],
            }
    md.update(md_calib)
    stream = to_event_model(data, output_info=output_info, md=md)

for nds in stream:
    #print(nds)
    x0, y0 = 720, 599
    rdet = 5
    #sin.emit(nds)
    stream_input(*nds)
    plt.pause(.1)
#    if nds[0] == 'start':
#        startdoc = nds[1].copy()
#        if 'detector_SAXS_x0_pix' not in startdoc:
#            startdoc['detector_SAXS_x0_pix'] = x0
#            startdoc['detector_SAXS_y0_pix'] = y0
#            startdoc['detector_SAXS_distance_m'] = rdet
#            nds = 'start', startdoc
#        run_next = False
#        for det in startdoc['detectors']:
#            if 'pilatus' in det:
#                run_next = True
#    if nds[0] == 'stop':
#        run_next = True
#    if run_next:
#        sin.emit(nds)
#    # refresh any plots to callbacks
#    if nds[0] == 'event':
#        plt.pause(.1)
