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
from SciStreams.config import config, masks as mask_config

# interfaces for saving to databroker
# NEED TO TURN INTO BLUESKY-LIKE CALLBACKS
from SciStreams.interfaces.plotting_mpl import plotting_mpl as iplotting
from SciStreams.interfaces.databroker import databroker as idb
from SciStreams.interfaces.file import file as ifile
from SciStreams.interfaces.xml import xml as ixml
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


# Will be replaced by more sophisticated masking routine later
def generate_mask(**kwargs):
    ''' right now just a bland mask generator.
        need to replace it with something more sophisticated when time comes

        ignores kwargs for now
    '''
    # TODO : don't make read everytime at the very least. but 
    # may require thought when distributing
    detector_key = kwargs.get('detector_key', None)
    print("det key", detector_key)
    BLEMISH_FNAME = BLEMISH_FILENAMES[detector_key]
    from PIL import Image
    mask = np.array(Image.open(BLEMISH_FNAME))
    return dict(mask=mask)


def set_detector_name(sdoc, detector_name='pilatus300'):
    sdoc['attributes']['detector_name'] = detector_name
    return StreamDoc(sdoc)


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
def filter_data(**kwargs):
    ''' Filter events data based on a certion condaition
        Here only choose two dimensional arrays.
    '''
    new_kwargs = dict()
    for key, val in kwargs.items:
        if hasattr(val, 'ndim') and val.ndim == 2:
            new_kwargs[key] = val
    return new_kwargs



import shed.event_streams as es
import streamz.core as sc
# this is meant to remove certain events 
# based on a predicate on descriptors
class filter_descriptor(es.EventStream):
    def __init__(self, predicate, *args, **kwargs):
        self.predicate = predicate

        super().__init__(*args, **kwargs)

    # this should just override output_info
    def descriptor(self, docs):
        output_info = list()
        for doc in docs:
            data_keys = doc['data_keys']
            for key, val in data_keys.items():
                if self.predicate(val):
                    output_info.append((key, val))
        self.output_info = output_info
        return super().descriptor(docs)



globaldict = dict()




# This is the main stream of data
# Stream setup, datbroker data comes here (a string uid)
sin = es.EventStream()

# TODO : run asynchronously?

###
# First split it off into attributes, but reading the metadata (Eventify)
# and passing to a calibration object
# push attributes into actual event data before we lose them
s_attributes = es.Eventify(sin)
# temporary stuff, until I patch it into with shed
from SciStreams.streams.XS_Streams import load_calib_dict,\
            _get_keymap_defaults, load_from_calib_dict
keymap, defaults = _get_keymap_defaults("cms")
def load_calib_dict_shed(keymap=None, defaults=None, **kwargs):
    return load_calib_dict(kwargs, keymap=keymap, defaults=defaults)
def load_from_calib_dict_shed(detector=None, calib_defaults=None, **kwargs):
    return load_from_calib_dict(kwargs, detector=detector,
            calib_defaults=calib_defaults)
s_calib = es.map(load_calib_dict_shed, s_attributes, keymap=keymap, defaults=defaults)
# TODO : make a "grab first detector" argument (looks for first data element
# that's an array and returns the name)
s_calib = es.map(load_from_calib_dict_shed, s_calib,
                      detector='pilatus300', calib_defaults=defaults,
                      output_info=(('calibration', {}),))
# now try caching, the object has its own builtin hashing that dask understands
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
#s_attributes.map(print)
#s_calib_obj.map(print)
#s_calib = es.map(load_from_calib, s_attributes, 
                 #input_info="")

# assume events coming in are a stream
s_data = sin
#s_image.map(print)
#s_event = es.Eventify(s_event, None)
s_data = filter_descriptor(lambda x : x['dtype'] == 'array', s_data)
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
s_detector_key = es.map(grab_first_det_key, s_data)
#s_out = sc.map(print, s_event)

s_mask = es.map(generate_mask, s_detector_key)

s_imgmaskcalib = es.zip(s_image, s_calib_obj, s_mask)

def get_stitch(**kwargs):
    return dict(stitchback=kwargs.get('stitchback', False))

def get_exposure(**kwargs):
    return dict(stitchback=kwargs.get('sample_exposure_time', None))

def get_origin(**kwargs):
    ''' get the origin from the attributes.'''
    x = kwargs.get('detector_SAXS_x0_pix', None)
    y = kwargs.get('detector_SAXS_y0_pix', None)
    return dict(origin=(y,x))

s_origin = es.map(get_origin, s_attributes)

s_exposure = es.map(get_exposure, s_attributes)

# move stitch to an event by grabbing from attributes
s_stitch = es.map(get_stitch, s_attributes)

def circavg_from_calibration_shed(*args, **kwargs):
    # wrap from current output to just a dict
    res = circavg_from_calibration(*args, **kwargs)
    return dict(**res.kwargs)

# merge streams to create zipped stream
from SciStreams.streams.XS_Streams import circavg_from_calibration
#def circavg_from_calibration(image, calibration, mask=None, bins=None):
s_circavg = es.map(circavg_from_calibration_shed, s_imgmaskcalib,
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
def qphiavg_shed(image, mask=None, bins=None, origin=None):
    res = qphiavg(image, mask=mask, bins=bins, origin=origin)
    # shed expects a dict
    return res.kwargs

s_img_mask_origin = es.map(lambda **kwargs : kwargs, es.zip(s_image, s_mask, s_origin),
                           input_info={'image' : ('image', 0),
                                       'mask' : ('mask', 1),
                                       'origin' : ('origin', 2)
                                       }
                           )
s_qphiavg = es.map(qphiavg_shed, s_img_mask_origin)
def grab_sqphi(**kwargs):
    return kwargs.get('sqphi', None)
s_sqphi = es.map(grab_sqphi, s_qphiavg,
        output_info=[('sqphi', {'filled' : True, 'shape' : ()})])
L = s_qphiavg.sink_to_list()



# attach to callbacks here
# eventually re-write data outputs in terms of callbacks
from bluesky.callbacks.broker import LiveImage
from bluesky.callbacks import LivePlot

liveimage_stitch = LiveImage('image')
s_stitched_image.sink(es.star(liveimage_stitch))
liveimage_sqphi = LiveImage('sqphi')
s_sqphi.sink(es.star(liveimage_sqphi))
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

    #hdrs = cmsdb(start_time="2017-09-01", stop_time="2017-09-07")
    hdrs = cmsdb(start_time="2017-07-14", stop_time="2017-07-15")
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
    sin.emit(nds)
    # refresh any plots to callbacks
    plt.pause(.1)
