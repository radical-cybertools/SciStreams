# test a XS run
import os

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
import SciAnalysis.config as config
from metadatastore.core import NoEventDescriptors
import numpy as np

# SciAnalysis imports
## interfaces
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.plotting_mpl import plotting_mpl as source_plotting
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.detectors import detectors2D
## Analyses
#from SciAnalysis.analyses.XSAnalysis.Protocols_streams import CircularAverage, Thumbnail
## Data
# the stream version
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Obstruction
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream, QPHIMapStream
from SciAnalysis.analyses.XSAnalysis.CustomStreams import SqFitStream


# Streams include stuff
# select needed for mapping
from SciAnalysis.interfaces.StreamDoc import StreamDoc, Stream, parse_streamdoc
#from streams.core import Stream


# Initialise Data objects
## Blemish file
blemish_filename = config.maskdir + "/Pilatus300k_main_gaps-mask.png"
blemish = source_file.FileDesc(blemish_filename).get_raw()[:,:,0] > 1
blemish = blemish.astype(int)
## prepare master mask import
SAXS_bstop_fname = "pilatus300_mastermask.npz"
res = np.load(config.maskdir + "/" + SAXS_bstop_fname)
SAXS_bstop_mask = res['master_mask']
SAXS_bstop_origin  = res['y0_master'], res['x0_master']
obs_SAXS = Obstruction(SAXS_bstop_mask, SAXS_bstop_origin)

GISAXS_bstop_fname = "mask_master_CMS_GISAXS_May2017.npz"
res = np.load(config.maskdir + "/" + GISAXS_bstop_fname)
GISAXS_bstop_mask = res['mask']
GISAXS_bstop_origin = res['origin']
obs_GISAXS = Obstruction(GISAXS_bstop_mask, GISAXS_bstop_origin)

obs_total = obs_GISAXS + obs_SAXS
obs3 =  obs_total - obs_SAXS

from pylab import *
ion()

# rows, cols
#origin = y0, x0
## Instantiate the MasterMask
master_mask = MasterMask(master=obs_total.mask, origin=obs_total.origin)
## Instantiate the master mask generator
mmg = MaskGenerator(master_mask, blemish)


# User defined parameters
## searching through databroker
dbname_data = "cms:data"
#search_kws = {
#'start_time' : "2017-04-01",
##'sample_savename' : 'PM-EG2876_recast',
#'sample_savename' : 'PM-EG2884_recast',
    #}
search_kws = {
'start_time' : "2017-01-01",
'sample_savename' : 'YT',
    }

data_uids = ['cdc07e39-c810-4fe7-b129-e769712d96f6',
        'f448325a-271e-418f-b99e-44d0b2dbb212',
        '25eeaf1c-c0e6-4feb-9d83-891e8ec385c3',
        '63c0e3e5-33a5-4559-96d5-7906a90677c6',
        'e6459f4f-eeaf-4c73-8ebf-2c50c30df01a',
        'a58f5674-a518-453b-bc80-d8b37afcdddd',
        '06d4a249-832f-4fb2-b5ef-6e76383551f1']

detector_key = 'pilatus300_image'
noqbins = None # If none, circavg will find optimal number

###### Done data loading ########


def print_cal(clb):
    print(clb)

from dask import compute

from functools import wraps

# This decorator is necessary to provide a way to find out what stream doc is
# unique. We need to tokenize only the args, kwargs and attributes of
# StreamDoc, nothing else
from SciAnalysis.interfaces.StreamDoc import _is_streamdoc
from dask.delayed import tokenize
def make_delayed_stream_dec(pure=True):
    def delayed_stream_dec(f):
        @wraps(f)
        def f_new(*args, **kwargs):
            hshlist_args = list()
            hshlist_kwargs = dict()
            # if it's a streamdoc, call tokenize
            # which will hash only args, kwargs and attributes
            # (not uid or stats)
            for arg in args:
                #print(arg)
                if _is_streamdoc(arg):
                    #print("is streamdoc")
                    hshlist_args.append(arg.tokenize())
                else:
                    #print("not streamdoc")
                    hshlist_args.append(arg)
            for k, v in kwargs.items():
                if _is_streamdoc(v):
                    hshlist_kwargs[k] = v.tokenize()
                else:
                    hshlist_kwargs[k] = v

            hsh = tokenize(*hshlist_args, **hshlist_kwargs)
            hsh = f.__name__ + "-" + hsh
            #print("function hash: {}".format(hsh))
            return delayed(f, pure=pure, name=hsh)(*args, **kwargs)
        return f_new
    return delayed_stream_dec

def delayed_wrapper(name):
    def decorator(f):
        @make_delayed_stream_dec(pure=True)
        @parse_streamdoc(name)
        @wraps(f)
        def f_new(*args, **kwargs):
            return f(*args, **kwargs)
        return f_new
    return decorator

def inspect_stream(stream):
    stream.apply(compute).apply(lambda x : print(x[0]))

def get_attributes(sdoc):
    # reasoning behind this: need to compute the attributes before they're
    # evaluated in function, else it gets passed as delayed reference
    def makesdoc(attr):
        return StreamDoc(args=[compute(attr)[0]])
    return makesdoc(sdoc['attributes'])

def add_attributes(sdoc, **attr):
    sdoc.add(attributes=attr)
    return sdoc

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

def get_stitch(attr, *args, **kwargs):
    #print(attr['stitch'])
    #print("kwargs : {}".format(kwargs))
    #print("args : {}".format(args))
    return attr['stitch'],

def get_exposure_time(attr, *args, **kwargs):
    return attr['sample_exposure_time']

def print_exposure(attr, **kwargs):
    try:
        print("exposure time from attr: {}".format(attr['sample_exposure_time']))
    except Exception:
        print("Cout not get exposure time")

def norm_exposure(image=None, exposure_time=None, **kwargs):
    return image/exposure_time

def multiply(A, B):
    return A*B

def divide(A, B):
    return A/B



globaldict = dict()
# This should be an interface
def save_image_recent(sdoc, fignum=None, name=None):
    global globaldict
    # make sure it's not delayed
    if fignum is None:
        fignum = 20
    sdoc = compute(sdoc)[0]
    if name is None:
        name = 'recent.jpg'
    img = sdoc['args'][0]
    plt.figure(fignum);
    #plt.title("log10img")
    #plt.imshow(np.log10(img))
    plt.title("img")
    plt.imshow((img))
    plt.savefig(name)

    globaldict[name] = img
    #from PIL import Image
    #im = Image.fromarray(img)
    #im.save(name)

def save_data(sdoc, name='default'):
    global globaldict
    sdoc = compute(sdoc)[0]
    a = sdoc['args'][0]
    globaldict[name] = a


# Stream setup, datbroker data comes here (a header for now)
sin = Stream(wrapper=delayed_wrapper)
s2 = sin.apply(lambda x : x.add_attributes(stream_name="InputStream"))#.apply(compute)
#sin.map(print, raw=True)
#s2.apply(compute).apply(lambda x : print(x[0]['attributes']['stream_name']))

#  separate data from attributes
attributes = s2.apply(get_attributes)
#attributes.apply(compute).apply(print)

# get image from the input stream
image = s2.select((detector_key,None))

# calibration setup
sin_calib, sout_calib = CalibrationStream(wrapper=delayed_wrapper)
#sin_calib.apply(print)
#sout_calib['q_maps'].apply(compute).apply(print)
calib_qmap = sout_calib['q_maps']
calibration = sout_calib['calibration']
origin = sout_calib['origin']
#origin.map(compute, raw=True)
#calib_qmap.map(compute, raw=True)
#calibration.map(compute, raw=True).map(print, raw=True)

# connect attributes to sin_calib
attributes.apply(sin_calib.emit)#.apply(compute)
#attributes.map(print_exposure).apply(compute)


# generate a mask
mskstr = origin.map(mmg.generate)

mask_stream = mskstr.select((0, 'mask'))
# compute it to test later
# TODO add when dask
#mask = mask_stream.(compute)

# circular average
sin_image_qmap = image.merge(calib_qmap, mask_stream)
sin_circavg, sout_circavg = CircularAverageStream(wrapper=delayed_wrapper)
sin_image_qmap.select(0, 'q_map', 'r_map', 'mask').apply(sin_circavg.emit)

# image stitching
stitch = attributes.map(get_stitch).select((0, 'stitch'))
exposure_time = attributes.map(get_exposure_time).select((0, 'exposure_time'))
exposure_mask = mask_stream.select(('mask', None))
exposure_mask = exposure_mask.merge(exposure_time.select(('exposure_time', None))).map(multiply)
exposure_mask = exposure_mask.select((0, 'mask'))

sin_imgstitch, sout_imgstitch = ImageStitchingStream(wrapper=delayed_wrapper)
sout_imgstitch_log = sout_imgstitch.select(('image', None)).map(np.log10).select((0, 'image'))
sout_imgstitch_log = sout_imgstitch_log.apply(lambda x : x.add_attributes(stream_name="ImgStitchLog"))
img_masked = image.merge(mask_stream.select(('mask',None))).map(multiply)
img_mask_origin = img_masked.select((0,'image')).merge(exposure_mask.select(('mask','mask')), origin.select((0, 'origin')), stitch)
img_mask_origin.apply(sin_imgstitch.emit)
#sin_imgstitch.apply(compute).apply(lambda x: print("printing : {}".format(x)))

sin_thumb, sout_thumb = ThumbStream(wrapper=delayed_wrapper, blur=1, resize=2)
image.apply(sin_thumb.emit)


# fitting
sqfit_in, sqfit_out = SqFitStream(wrapper=delayed_wrapper)
sout_circavg.apply(sqfit_in.emit)


# qphi map NOT WOKRING
#sqphi_in, sqphi_out = QPHIMapStream(wrapper=delayed_wrapper)
#image.merge(mask_stream, origin.select(0, 'origin')).apply(sqphi_in.emit)

#sout_thumb.apply(client.compute).sink_to_deque()


# some plotting/sinks
#mask_stream.select(('mask', None)).apply(save_image_recent,fignum=21, name="mask.png")#.apply(compute)
#image.apply(save_image_recent, fignum=22, name="image.png").apply(compute)
#sout_imgstitch.select(('image', None)).apply(save_image_recent,fignum=23, name="stitched.png")#.apply(compute)
#image.apply(save_image_recent, fignum=24, name="img_time.png").apply(compute)
#sout_imgstitch.select(('mask', None)).apply(save_image_recent,fignum=25, name="stitch-mask.png")#.apply(compute)
#sout_imgstitch.select(('origin', None)).apply(save_data, name="origin")#.apply(compute)

# save to file system
sout_circavg.apply(compute).apply(lambda x : x[0]).apply(source_plotting.store_results, lines=[('sqx', 'sqy')],\
                                                        scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$",
                                                        ylabel="I(q)")
sout_imgstitch.apply(compute).apply(lambda x : x[0]).apply(source_plotting.store_results, images=['image'], hideaxes=True)
sout_imgstitch_log.apply(compute).apply(lambda x : x[0]).apply(source_plotting.store_results, images=['image'], hideaxes=True)
sout_thumb.apply(compute).apply(lambda x : x[0]).apply(source_plotting.store_results, images=['thumb'], hideaxes=True)
sout_thumb.select(('thumb', None)).map(np.log10).select((0,'thumb'))\
        .apply(lambda x : x.add_attributes(stream_name="ThumbLog")).apply(compute)\
        .apply(lambda x : x[0])\
        .apply(source_plotting.store_results, images=['thumb'], hideaxes=True)

sqfit_out.apply(compute).apply(lambda x :
                               x[0]).apply(source_plotting.store_results,
                                           lines=[('sqx', 'sqy'), ('sqx', 'sqfit')],
                                           scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$", ylabel="I(q)")
# TODO : fix
#sqphi_out.select(('sqphi', None)).map(np.log10).select((0, 'sqphi'))\
        #.apply(compute).apply(lambda x :
                               #x[0]).apply(source_plotting.store_results,
                                           #images=['sqphi'], xlabel="$\phi$", ylabel="$q$")
#


#sdoc = source_databroker.pullrecent("cms:data", start_time="2017-04-01", stop_time="2017-04-03")
#sdoc = delayed(sdoc)
#sdoc_gen = source_databroker.search("cms:data", start_time="2017-01-01", stop_time="2017-04-01", sample_savename="YT")

# read uids directly rather than search (search is slow, bypass for now)
import os
filename = os.path.expanduser("~/SciAnalysis-data/storage/YT_uids.txt")
data_uids = list()
with open(filename,"r") as f:
    for ln in f:
        ln = ln.strip("\n")
        data_uids.append(ln)


# Emitting data

#sdoc_gen = source_databroker.search(dbname_data, **search_kws)

sdoc_gen = source_databroker.pullfromuids(dbname_data, data_uids)

cnt = 0
for sdoc in sdoc_gen:
    if cnt == 0:
        sdoc['attributes']['stitch'] = 0
    else:
        sdoc['attributes']['stitch'] = 1

    cnt +=1

    ## TODO : emit should maybe transform according to wrapper?
    # add delayed wrapper
    #sdoc._wrapper = delayed
    sin.emit(delayed(sdoc))




# plot results
#img = globaldict['stitched.png']
#mask = globaldict['stitch-mask.png']
#plt.figure(25);
#plt.clf();
#plt.imshow(img/mask)


''' FAQ :
    Setting an array element with a sequence

    list index out of range : check that you have correct args for function (using select) routine
        inspect_stream at that point

    circavg() missing 1 required positional argument: 'image'
        you need to make sure the input arguments match output. use select routine to help
        use inspect_stream to inspect stream at that point


    some errors like "KeyError" may follow an initial error. make sure to
        backtrace your errors all the way

    TypeError: tuple indices must be integers or slices, not str
        usually dask stuff. Did you run "compute()" this returns a tuple
        also, make sure you don't zip streamdocs but merge them

    common problems :
        - args/kwargs mapping
        - zip not merge
        - etc...

    some function being ignored? maybe you redefined stream input!
        ex : sin = Stream()
        sin = sin.apply(somefunction)
        When you emit, this is not longer to top of stream; it will ignore that
        apply function

    argument number problems? did you remember to add the wrapper?

/home/lhermitte/conda_envs/py3/lib/python3.5/site-packages/tornado/gen.py in run(self)
   1019 
   1020                     if exc_info is not None:
-> 1021                         yielded = self.gen.throw(*exc_info)
   1022                         exc_info = None
   1023                     else:

/home/lhermitte/projects/distributed/distributed/client.py in _gather(self, futures, errors)
    938                             six.reraise(type(exception),
    939                                         exception,
--> 940                                         traceback)
    941                     if errors == 'skip':
    942                         bad_keys.add(key)

/home/lhermitte/conda_envs/py3/lib/python3.5/site-packages/six.py in reraise(tp, value, tb)
    683             value = tp()
    684         if value.__traceback__ is not tb:
--> 685             raise value.with_traceback(tb)
    686         raise value
    687 

/home/lhermitte/projects/distributed/distributed/protocol/pickle.py in loads()
     57 def loads(x):
     58     try:
---> 59         return pickle.loads(x)
     60     except Exception:
     61         logger.info("Failed to deserialize %s", x[:10000], exc_info=True)

/home/lhermitte/conda_envs/py3/lib/python3.5/site-packages/numpy/core/__init__.py in _ufunc_reconstruct()
     70     # scipy.special.expit for instance.
     71     mod = __import__(module, fromlist=[name])
---> 72     return getattr(mod, name)
     73 
     74 def _ufunc_reduce(func):

AttributeError: module '__mp_main__' has no attribute 'log'


'''
