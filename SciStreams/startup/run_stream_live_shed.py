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
MASK_DIR = "~/research/projects/SciAnalysis-data/masks"
MASK_DIR = os.path.expanduser(MASK_DIR)
BLEMISH_pilatus2M = MASK_DIR + "/pilatus2M_image/blemish_pilatus2M_Sept2017.tif"
BLEMISH_pilatus300 = MASK_DIR + "/pilatus300_image/pilatus300_mask_main.png"

BLEMISH_FILENAMES = dict(
        pilatus300_image=BLEMISH_pilatus300,
        pilatus2M_image=BLEMISH_pilatus2M,
        )

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
from SciStreams.data.Obstructions import Obstruction
from SciStreams.data.Mask import BeamstopXYPhi, MaskFrame

# if using dask async stuff will need this again
#from tornado.ioloop import IOLoop
#from tornado import gen

from SciStreams.callbacks import CallbackBase
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


from SciStreams.detectors import detectors2D
# Will be replaced by more sophisticated masking routine later
def get_mask(**kwargs):
    ''' right now just a bland mask generator.
        need to replace it with something more sophisticated when time comes

        ignores kwargs for now
    '''
    # TODO : don't make read everytime at the very least. but 
    # may require thought when distributing
    detector_key = kwargs.get('detector_key', None)
    # remove last "_" character
    detector_name = detector_key[::-1].split("_", maxsplit=1)[-1][::-1]
    mask_generator = detectors2D[detector_name]['mask_generator']['value']
    mask = mask_generator(**kwargs)
    return dict(mask=mask)

#MASK_GENERATORS = {'pilatus2M_image' : generate_mask_pilatus2M,
                   #'pilatus300_image' : generate_mask_pilatus300
                   #}



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




# This is the main stream of data
# Stream setup, datbroker data comes here (a string uid)
sin = es.EventStream()
sin_primary = es.filter(lambda x : x[0]['name'] == 'primary', sin,
                        document_name='descriptor')


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
s_attributes = es.Eventify(sin)
# set the detector name from det key in start (adding metadata)
s_attributes = es.map(get_detector_key_start, s_attributes)

s_attributes = es.map(normalize_calib_dict, s_attributes, keymap_name='cms')
s_attributes = es.map(add_detector_info, s_attributes)

from SciStreams.detectors.mask_generators import generate_mask




# TODO : The detector key should be taken from descriptor
# TODO : This assumes events for images are length 1. If more
# than 1, need to repeat the s_calib stream (which is just 1 event)
# first pick the detector, just the first one for now
# TODO : add splits of stream to compute for multiple detectors
# next make a calibration object
s_calib = es.map(make_calibration, s_attributes, output_info=[('calibration', {})])

# TODO : make a "grab first detector" argument (looks for first data element
# that's an array and returns the name)
#
## now try caching, the object has its own builtin hashing that dask understands
def _generate_qxyz_maps(calibration):
    calibration.generate_maps()
    return calibration
from SciStreams.globals import client
import SciStreams.globals as streams_globals
s_calib_obj = es.map(lambda calibration:
                          client.submit(_generate_qxyz_maps, calibration),
                          s_calib,
                          output_info=(('calibration', {}),))
es.map(lambda calibration : streams_globals.futures_cache.append(calibration),
        s_calib_obj)
s_calib_obj = s_calib_obj.map(lambda x: client.gather(x))
# END Calibration

## assume events coming in are a stream
s_data = sin_primary
#s_image.map(print)
#s_event = es.Eventify(s_event, None)
# TODO : synchronize this with start doc (maybe choose only the data key
# corresponding to metadata)
s_data = es.map(pick_arrays, s_data)
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

s_image = es.map(grab_first_det, s_data)
# TODO : synchronize with start doc
s_detector_key = es.map(grab_first_det_key, s_data)
#s_out = sc.map(print, s_event)

#s_attributes.map(print)
s_mask = es.map(generate_mask, s_attributes)

s_imgmaskcalib = es.zip(s_image, s_calib_obj, s_mask)

def get_stitch(**kwargs):
    return dict(stitchback=kwargs.get('stitchback', False))

def get_exposure(**kwargs):
    return dict(stitchback=kwargs.get('sample_exposure_time', None))

def get_origin(**kwargs):
    ''' get the origin from the attributes.'''
    x = kwargs.get('detector_SAXS_x0_pix', None)
    y = kwargs.get('detector_SAXS_y0_pix', None)
    if x is None or y is None:
        origin = None
    else:
        origin = (y,x)

    return dict(origin=origin)

s_origin = es.map(get_origin, s_attributes)

s_exposure = es.map(get_exposure, s_attributes)

# move stitch to an event by grabbing from attributes
s_stitch = es.map(get_stitch, s_attributes)

# merge streams to create zipped stream
from SciStreams.streams.XS_Streams import circavg_from_calibration
#def circavg_from_calibration(image, calibration, mask=None, bins=None):
s_circavg = es.map(circavg_from_calibration, s_imgmaskcalib,
                   input_info={'image' : ('image', 0),
                               'calibration' : ('calibration', 1),
                               'mask' : ('mask', 2),
                    }
        )


def pack_args(image, mask, origin, stitchback):
    ''' need to make into a tuple.'''
    return (image, mask, origin, stitchback)

def xystitch_result_shed(curstate):
    img_acc, mask_acc, origin_acc, stitchback_acc = curstate
    return xystitch_result(img_acc=img_acc, mask_acc=mask_acc,
                           origin_acc=origin_acc,
                           stitchback_acc=stitchback_acc)


from SciStreams.processing.stitching import xystitch_accumulate, xystitch_result
s_stitched = es.map(pack_args, es.zip(s_image, s_mask, s_origin,
                                        s_stitch),
                    input_info={'image' : ('image', 0),
                                'mask' : ('mask', 1),
                                'origin' : ('origin', 2),
                                'stitchback' : ('stitchback', 3)
                               }, output_info=[('state',  {})])
s_stitched = es.accumulate(es.dstar(xystitch_accumulate), s_stitched,
        input_info={'newstate' : 'state'}, output_info=[('state', {})],
        state_key='prevstate')

s_stitched = es.map(xystitch_result_shed, s_stitched,
                    input_info={'curstate' : 'state'})

def grab_stitched_image(image, mask, origin, stitchback):
    return image

s_stitched_image = es.map(grab_stitched_image, s_stitched,
                            output_info=[('image', {'filled' : True,
                                                #'shape' : (1679, 1475)})])
                                                'shape' : (1679, 1475)})])

from SciStreams.processing.qphiavg import qphiavg

s_img_mask_origin = es.map(lambda **kwargs : kwargs, es.zip(s_image, s_mask, s_origin),
                           input_info={'image' : ('image', 0),
                                       'mask' : ('mask', 1),
                                       'origin' : ('origin', 2)
                                       }
                           )
s_qphiavg = es.map(qphiavg, s_img_mask_origin, bins=(800,360))
def grab_sqphi(**kwargs):
    return kwargs.get('sqphi', None)
s_sqphi = es.map(grab_sqphi, s_qphiavg,
        output_info=[('sqphi', {'filled' : True, 'shape' : ()})])
L = s_qphiavg.sink_to_list()



# attach to callbacks here
# eventually re-write data outputs in terms of callbacks
# some callbacks are just bluesky imports. Also all callbacks inherit bluesky
# templates. Maybe it may be better to remove bluesky dep in future?
from SciStreams.callbacks.live import LiveImage, LivePlot

# TODO : use kde's for normalization
def image_norm(image, low=.05, high=.95):
    ''' this is just threshold normalization
        Assumes positive valued images
    '''
    img = image
    data = img[~np.isnan(img)*~np.isinf(img)]
    nobins = int(np.max(data))
    hh, bin_edges = np.histogram(data, bins=nobins)
    cumcnts = np.cumsum(hh)
    hhsum = np.sum(hh)


    # only threshold if there are counts
    if hhsum > 0:
        cumcnts = cumcnts/hhsum
        wlow = np.where(cumcnts > low)[0][0]
        whigh = np.where(cumcnts < high)[0][-1]
        img = ((img>wlow) < whigh)*img
        img[np.isnan(img)+np.isinf(img)] = 0

    return image



liveimage_stitch = LiveImage('image')
es.map(image_norm, s_stitched_image, output_info=[('image', {})]).sink(es.star(liveimage_stitch))
liveimage_sqphi = LiveImage('sqphi', aspect='auto')
es.map(image_norm, s_sqphi, input_info={'image' : 'sqphi'},
        output_info=[('sqphi', {})]).sink(es.star(liveimage_sqphi))
liveimage_sqplot = LivePlot('sqy', x='sqx')
s_circavg.sink(es.star(liveimage_sqplot))







#figure(2);clf()
#es.map(lambda mask: dict(foo=imshow(mask)), s_mask)

# next start working on result
#s_event = s_event\
        #.map(add_attributes, stream_name="InputStream")
#s_event = s_event\
        #.map(set_detector_name, detector_name='pilatus300')

if True:
    # patchy way to get stream for now, need to fix later
    from SciStreams.interfaces.databroker.databases import databases

    cmsdb = databases['cms:data']

    hdrs = cmsdb(start_time="2017-07-13", stop_time="2017-07-14")# 16:00")
    #hdrs = cmsdb(start_time="2017-07-14", stop_time="2017-07-15")
    stream = cmsdb.restream(hdrs, fill=True)

elif True:
    # simulate the data
    from shed.utils import to_event_model
    Ndata = 2
    width = 1475
    height = 1679
    data = np.random.random((Ndata, height, width))
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
            }
    md.update(md_calib)
    stream = to_event_model(data, output_info=output_info, md=md)

for nds in stream:
    print(nds)
    x0, y0 = 720, 599
    rdet = 5
    if nds[0] == 'start':
        startdoc = nds[1].copy()
        if 'detector_SAXS_x0_pix' not in startdoc:
            startdoc['detector_SAXS_x0_pix'] = x0
            startdoc['detector_SAXS_y0_pix'] = y0
            startdoc['detector_SAXS_distance_m'] = rdet
            nds = 'start', startdoc
        run_next = False
        for det in startdoc['detectors']:
            if 'pilatus' in det:
                run_next = True
    if nds[0] == 'stop':
        run_next = True
    if run_next:
        sin.emit(nds)
    # refresh any plots to callbacks
    if nds[0] == 'event':
        plt.pause(.1)
