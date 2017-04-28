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

###### Done data loading ########


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

def get_attributes(sdoc):
    return sdoc.get_attributes()

import matplotlib.pyplot as plt
plt.ion()
plt.clf()

def plot_line(*args, **kwargs):
    plt.figure(2);plt.clf();
    plt.plot(*args)
    plt.pause(.1)

# TODO : make sure saving is not pure=True!
def plot_image(img):
    plt.figure(1)
    plt.clf();plt.imshow(img)
    plt.draw();plt.pause(.5);

def get_image(imgtmp):
    global img
    img = imgtmp

def get_mask(imgtmp):
    global mask
    mask = imgtmp

# This should be an interface
def save_image_recent(sdoc, name=None):
    # make sure it's not delayed
    sdoc = compute(sdoc)[0]
    if name is None:
        name = 'recent.jpg'
    img = sdoc['args'][0]
    from PIL import Image
    im = Image.fromarray(img)
    im.save(name)


# Stream setup, datbroker data comes here (a header for now)
sin = Stream(wrapper=delayed_wrapper)
# separate data from attributes
attributes = sin.apply(get_attributes)


image = sin.select((detector_key,None))

# calibration
sin_calib, sout_calib = CalibrationStream(wrapper=delayed_wrapper)
calib_qmap = sout_calib['q_maps']
calibration = sout_calib['calibration']
origin = sout_calib['origin']

# connect attributes to sin_calib
attributes.apply(sin_calib.emit)

# generate a mask
mskstr = origin.map(mmg.generate)
mask_stream = mskstr.select((0, 'mask'))
# compute it to test later
mask = mask_stream.apply(compute)

# image goes here, one argument
#sin_image = Stream(wrapper=delayed_wrapper)
# first merge qmap with image, then erge mask_stream, but select component of
# mask_stream first
sin_image_qmap = image.merge(calib_qmap, mask_stream)

sin_circavg, sout_circavg = CircularAverageStream(wrapper=delayed_wrapper)
#s_circavgcalib.apply(compute).apply(print)
sin_image_qmap.apply(sin_circavg.emit)

#inspect_stream(sout_circavg)
sout_circavg.select([('sqx', None), ('sqy', None)]).map(plot_line).apply(compute)
image.map(plot_image).apply(compute)
image.apply(save_image_recent, name="image.png")



def multiply(img, mask):
    return img*mask

sin_imgstitch, sout_imgstitch = ImageStitchingStream(wrapper=delayed_wrapper)
img_masked = image.merge(mask_stream.select(('mask',None))).map(multiply)
img_mask_origin = img_masked.select((0,'image')).merge(mask_stream.select(('mask','mask')), origin.select((0, 'origin')))
img_mask_origin.apply(sin_imgstitch.emit)



#mask.map(plot).apply(compute)



# Add some inspection routines in the stream

sout_imgstitch.select(('image',None)).map(plot_image).apply(compute)
sout_imgstitch.select(('image',None)).map(get_image).apply(compute)
sout_imgstitch.select(('mask',None)).map(get_mask).apply(compute)
#sout_imgstitch.select(('image', None)).apply(save_image_recent, name="stitched.png")
#imgstitch_sink.select(0).map(plot_image).apply(compute)
#imgstitch_sink.select(0).map(get_image).apply(compute)
#imgstitch_sink.select(1).map(get_mask).apply(compute)

#mask_stream.select(0).map(plot_image).apply(compute)
#mask_stream.select(0).map(get_image).apply(compute)
#mask_stream.select(('mask', None)).map(get_mask).apply(compute)

# emitting of data
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



#sdoc = source_databroker.pullrecent("cms:data", start_time="2017-04-01", stop_time="2017-04-03")
#sdoc = delayed(sdoc)
sdoc_gen = source_databroker.search("cms:data", start_time="2017-01-01", stop_time="2017-04-01", sample_savename="YT")


for sdoc in sdoc_gen:
    sin.emit(delayed(sdoc))
## input needs to be same as what wrapper works on
## TODO : emit should maybe transform according to wrapper?
#
#arr=np.random.uniform(size=(619, 487))
#sdoc = delayed(StreamDoc(args=arr))
#sin_image.emit(sdoc)
#
#attributes = attributes.copy()
#attributes['detector_SAXS_x0_pix']=300
#sdoc = delayed(StreamDoc(args=attributes))
#sin_calib.emit(sdoc)
## input needs to be same as what wrapper works on
## TODO : emit should maybe transform according to wrapper?
#
#arr=np.random.uniform(size=(619, 487))
#sdoc = delayed(StreamDoc(args=arr))
#sin_image.emit(sdoc)
#
#
#attributes = attributes.copy()
#attributes['detector_SAXS_y0_pix']=110
#sdoc = delayed(StreamDoc(args=attributes))
#sin_calib.emit(sdoc)
## input needs to be same as what wrapper works on
## TODO : emit should maybe transform according to wrapper?
#
#arr=np.random.uniform(size=(619, 487))
#sdoc = delayed(StreamDoc(args=arr))
#sin_image.emit(sdoc)
#
#


''' FAQ :
    Setting an array element with a sequence

    list index out of range : check that you have correct args for function (using select) routine
        inspect_stream at that point

    circavg() missing 1 required positional argument: 'image'
        you need to make sure the input arguments match output. use select routine to help
        use inspect_stream to inspect stream at that point


    '''
