# test a XS run

# dask imports
# set up the distributed client
from dask.cache import Cache
_cache = Cache(1e9)
_cache.register()

#from distributed import Client
#client = Client("10.11.128.3:8786")

from dask import set_options, delayed, compute
# assume all functions are pure globally
set_options(delayed_pure=False)

# misc imports
import sys
from config import MASKDIR
from metadatastore.core import NoEventDescriptors
import numpy as np

# SciAnalysis imports
## interfaces
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.detectors import detectors2D
from SciAnalysis.interfaces.SciResult import SciResult
## Analyses
#from SciAnalysis.analyses.XSAnalysis.Protocols_streams import CircularAverage, Thumbnail
## Data
# the stream version
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream

# Streams include stuff
from SciAnalysis.streams.StreamDoc import StreamDoc, parse_streamdoc
from SciAnalysis.streams.core import Stream

# Initialise Data objects
## Blemish file
blemish_filename = MASKDIR + "/Pilatus300k_main_gaps-mask.png"
blemish = source_file.FileDesc(blemish_filename).get_raw()[:,:,0] > 1
blemish = blemish.astype(int)
## prepare master mask import
master_mask_name = "pilatus300_mastermask.npz"
master_mask_filename = MASKDIR + "/" + master_mask_name
res = np.load(master_mask_filename)
master_mask= res['master_mask']
x0, y0 = res['x0_master'], res['y0_master']
# rows, cols
origin = y0, x0
## Instantiate the MasterMask
master_mask = MasterMask(master=master_mask, origin=origin)
## Instantiate the master mask generator
mmg = MaskGenerator(master_mask, blemish)


# User defined parameters
## searching through databroker
dbname_data = "cms:data"
search_kws = {
'start_time' : "2017-01-01",
'sample_savename' : 'YT',
    }
detector_key = 'pilatus300_image'
noqbins = None # If none, circavg will find optimal number

# some global attributes to inherit
global_attrs = [
        "sample_name",
        "sample_savename",
        "data_uid",
        "scan_id",
        "experiment_alias_directory",
        "experiment_SAF_number",
        "experiment_group",
        "experiment_cycle",
        "experiment_project",
        "experiment_proposal_number",
        "experiment_type",
        "experiment_user",
        "filename",
        "measure_type",
        ]

###### Done data loading ########

sdoc = StreamDoc()
sdoc['attributes'] = scires = SciResult()
sdoc['attributes'] = dict(
        calibration_wavelength_A=1.,
        detector_SAXS_x0_pix=400,
        detector_SAXS_y0_pix=300,
        detector_SAXS_distance_m=5.,
)
# add a random image
sdoc.add(np.random.uniform((500,500)))
attributes = sdoc['attributes']

def print_cal(clb):
    print(clb)

from dask import compute

from functools import wraps

def delayed_wrapper(name):
    def decorator(f):
        @delayed(pure=True)
        @parse_streamdoc(name)
        @wraps(f)
        def f_new(*args, **kwargs):
            return f(*args, **kwargs)
        return f_new
    return decorator


def inspect_stream(stream):
    stream.apply(compute).apply(lambda x : print(x[0]))

source_calib, sinks_calib = CalibrationStream(wrapper=delayed_wrapper)
# TEST
#new_sinks_calib = sinks_calib['q_map'].apply(select, (0, 'q_map')).apply(compute).apply(print)
calib_qmap = sinks_calib['q_maps']
calibration = sinks_calib['calibration']
origin = sinks_calib['origin']

# generate a mask
mskstr = origin.map(mmg.generate)
mask_stream = mskstr.select((0, 'mask'))
# compute it to test later
mask = mask_stream.apply(compute)
#sinks['origin'].apply(compute).apply(print)

# image goes here, one argument
source_image = Stream(wrapper=delayed_wrapper)
# first merge qmap with image, then erge mask_stream, but select component of
# mask_stream first
source_image_qmap = source_image.merge(calib_qmap, mask_stream)
#source_image_qmap.apply(compute).apply(print)

source_circavg, sink_circavg = CircularAverageStream(wrapper=delayed_wrapper)
#s_circavgcalib = sinks_calib['calibration'].merge(sink_circavg)
#s_circavgcalib.apply(compute).apply(print)
source_image_qmap.apply(source_circavg.emit)
#sink_circavg.apply(compute).apply(print)

#inspect_stream(source_image)

imgstitch_source, imgstitch_sink = ImageStitchingStream(wrapper=delayed_wrapper)
img_mask_origin = source_image.select((0,'image')).merge(mask_stream.select(('mask','mask')), origin.select((0, 'origin')))
img_mask_origin.apply(imgstitch_source.emit)


import matplotlib.pyplot as plt
plt.ion()
plt.clf()


def plot(*args, **kwargs):#mask=None):
    print(kwargs)
    #print(mask.shape)
    #plt.clf();plt.imshow(mask)

# TODO : make sure saving is not pure=True!
def plot_image(img):
    #img = args[0]
    print(img.shape)
    plt.figure(1)
    plt.clf();plt.imshow(img)
    plt.draw();plt.pause(.1)

def get_image(imgtmp):
    global img
    img = imgtmp

def get_mask(imgtmp):
    global mask
    mask = imgtmp

#mask.map(plot).apply(compute)


#res = sinks['qx_map'].apply(compute)
#res.sink(print)

inspect_stream(imgstitch_sink)

#imgstitch_sink.select(('image',None)).map(plot_image).apply(compute)
#imgstitch_sink.select(('image',None)).map(get_image).apply(compute)
#imgstitch_sink.select(('mask',None)).map(get_mask).apply(compute)
imgstitch_sink.select(0).map(plot_image).apply(compute)
imgstitch_sink.select(0).map(get_image).apply(compute)
imgstitch_sink.select(1).map(get_mask).apply(compute)

# emitting of data
sdoc = delayed(StreamDoc(args=attributes))
source_calib.emit(sdoc)
# input needs to be same as what wrapper works on
# TODO : emit should maybe transform according to wrapper?

arr=np.random.uniform(size=(619, 487))
sdoc = delayed(StreamDoc(args=arr))
source_image.emit(sdoc)

attributes = attributes.copy()
attributes['detector_SAXS_x0_pix']=300
sdoc = delayed(StreamDoc(args=attributes))
source_calib.emit(sdoc)
# input needs to be same as what wrapper works on
# TODO : emit should maybe transform according to wrapper?

arr=np.random.uniform(size=(619, 487))
sdoc = delayed(StreamDoc(args=arr))
source_image.emit(sdoc)


attributes = attributes.copy()
attributes['detector_SAXS_y0_pix']=500
sdoc = delayed(StreamDoc(args=attributes))
source_calib.emit(sdoc)
# input needs to be same as what wrapper works on
# TODO : emit should maybe transform according to wrapper?

arr=np.random.uniform(size=(619, 487))
sdoc = delayed(StreamDoc(args=arr))
source_image.emit(sdoc)


