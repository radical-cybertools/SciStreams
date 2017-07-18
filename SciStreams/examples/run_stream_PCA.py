import os, sys

# dask imports (like Spark)
from dask.cache import Cache
from dask import set_options, delayed, compute
set_options(delayed_pure=True)
_cache = Cache(1e9)
_cache.register()

# general python objects to intercept some data
from collections import deque

# numerical libaries
import numpy as np

# SciAnalysis configuration imports
from SciAnalysis.globals import client
# this looks for a YAML file with cluster configuration
# currently my home dir (but should work in my running instance of jupyter)
# assume all functions are pure globally
import SciAnalysis.config as config

# SciAnalysis imports for the various interfaces
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.plotting_mpl import plotting_mpl as source_plotting
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.detectors import detectors2D
# The StreamDoc
from SciAnalysis.interfaces.StreamDoc import StreamDoc, Stream, parse_streamdoc, delayed_wrapper

# Some pre-built streams
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Obstruction
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream, QPHIMapStream
from SciAnalysis.analyses.XSAnalysis.CustomStreams import SqFitStream, PCAStream

# these are more for debugging
# temporary database setup
from SciAnalysis.interfaces.databroker.databases import databases
cddb = databases['cms:data']
from pylab import *
ion()
import dask
def compute(obj):
    ''' slight modification to remove this tuple being returned.'''
    return dask.compute(obj)[0]


######### Initialize a mask generator
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

master_mask = MasterMask(master=obs_total.mask, origin=obs_total.origin)
## Instantiate the master mask generator
mmg = MaskGenerator(master_mask, blemish)
######### Done


########## Some convenience routines
def isSAXS(sdoc):
    ''' return true only if a SAXS expt.'''
    attr = sdoc['attributes']
    if 'experiment_type' in attr:
        #print("experiment type : {}".format(attr['experiment_type']))
        expttype = attr['experiment_type']
        if expttype == 'SAXS' or expttype == 'TSAXS':
            return True
    return False

def check_stitchback(sdoc):
    if 'stitchback' not in sdoc['attributes']:
        sdoc['attributes']['stitchback'] = 0
    return StreamDoc(sdoc)

def add_attributes(sdoc, **attr):
    # make a copy
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc

def set_detector_name(sdoc, detector_name='pilatus300'):
    sdoc['attributes']['detector_name'] = detector_name
    return StreamDoc(sdoc)

def get_attributes(sdoc):
    # reasoning behind this: need to compute the attributes before they're
    # evaluated in function, else it gets passed as delayed reference
    return StreamDoc(args=sdoc['attributes'])

def pack(*args):
    return args

def get_stitchback(attr, *args, **kwargs):
    return attr['stitchback']

def get_exposure_time(attr, *args, **kwargs):
    return attr['sample_exposure_time']
##########

# Stream setup. Input : a string uid for the data. Specify database with dbname
sin = Stream(wrapper=delayed_wrapper)
# functions are mapped or applied
# pull from a uid
s_event = sin.buffer(100).apply(source_databroker.pullfromuid, dbname='cms:data')
# filter for SAXS experiments. This is locally performed
s_event = s_event.apply(compute).filter(isSAXS).apply(delayed)
s_event = s_event.apply(delayed(check_stitchback))
# name the stream (needs to be improved)
s_event = s_event.apply(delayed(add_attributes), stream_name="InputStream")#.apply(compute)
# choose the detector (needs to be generalized to compute for all detectors,
# but I am waiting on DAMA to integrate WAXS with bluesky
s_event = s_event.apply(delayed(set_detector_name), detector_name='pilatus300')

#  separate data from attributes
attributes = s_event.apply(delayed(get_attributes))

# get image from the input stream
image = s_event.apply(lambda x : delayed(x.select)((detector_key,None)))

# calibration setup
sin_calib, sout_calib = CalibrationStream(wrapper=delayed_wrapper)
# get origin, but swap height, width
origin = sout_calib.map(lambda x : (x.origin[1], x.origin[0]))

# connect attributes to sin_calib
# connect streams together
attributes.apply(sin_calib.emit)#.apply(compute)
sink1 = deque()
attributes.apply(compute).apply(sink1.append)

# generate a mask
mskstr = origin.map(mmg.generate)

mask_stream = mskstr.select((0, 'mask'))

# circular average
sin_image_qmap = image.merge(sout_calib, mask_stream)
sin_circavg, sout_circavg = CircularAverageStream(wrapper=delayed_wrapper)
# connect streams together
sin_image_qmap.select(0, 1, 'mask').apply(sin_circavg.emit)
#sout_circavg.apply(compute).apply(print)

# image stitching
stitch = attributes.map(get_stitchback).select((0, 'stitchback'))
exposure_time = attributes.map(lambda x : x['sample_exposure_time']).select((0, 'exposure_time'))
exposure_mask = mask_stream.select(('mask', None))
exposure_mask = exposure_mask.merge(exposure_time.select(('exposure_time', None))).map(multiply)
exposure_mask = exposure_mask.select((0, 'mask'))

sin_imgstitch, sout_imgstitch = ImageStitchingStream(wrapper=delayed_wrapper)
#sout_imgstitch.apply(print).apply(compute)
sout_imgstitch_log = sout_imgstitch.select(('image', None)).map(np.log10).select((0, 'image'))
sout_imgstitch_log = sout_imgstitch_log.apply(delayed(add_attributes), stream_name="ImgStitchLog")
img_masked = image.merge(mask_stream.select(('mask',None))).map(multiply)
img_mask_origin = img_masked.select((0,'image')).merge(exposure_mask.select(('mask','mask')), origin.select((0, 'origin')), stitch)
img_mask_origin.apply(sin_imgstitch.emit)
#sin_imgstitch.apply(compute).apply(lambda x: print("printing : {}".format(x)))

# blur by 1 pix and bin image by 4x4 pixels
sin_thumb, sout_thumb = ThumbStream(wrapper=delayed_wrapper, blur=1, resize=4)
image.apply(sin_thumb.emit)

sin_pca, sout_pca = PCAStream(wrapper=delayed_wrapper, partition_size=100, n_components=10)
# connet the thumbnail output to PCA
sout_thumb.apply(sin_pca.emit)


# fitting
sqfit_in, sqfit_out = SqFitStream(wrapper=delayed_wrapper)
sout_circavg.apply(sqfit_in.emit)


sqphi_in, sqphi_out = QPHIMapStream(wrapper=delayed_wrapper)
image.merge(mask_stream, origin.select((0, 'origin'))).apply(sqphi_in.emit)


############## Saving to plots
resultsqueue = deque(maxlen=1000)
sout_circavg.apply(delayed(source_plotting.store_results), lines=[('sqx', 'sqy')],\
                   scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$",
                   ylabel="I(q)").apply(client.compute).apply(resultsqueue.append)
sout_imgstitch.apply(delayed(source_plotting.store_results), images=['image'], hideaxes=True).apply(client.compute).apply(resultsqueue.append)
sout_imgstitch_log.apply(delayed(source_plotting.store_results), images=['image'], hideaxes=True).apply(client.compute).apply(resultsqueue.append)
sout_thumb.apply(delayed(source_plotting.store_results), images=['thumb'], hideaxes=True).apply(client.compute).apply(resultsqueue.append)
sout_thumb.select(('thumb', None)).map(np.log10).select((0,'thumb'))\
        .apply(delayed(add_attributes), stream_name="ThumbLog")\
        .apply(delayed(source_plotting.store_results), images=['thumb'], hideaxes=True)\
        .apply(client.compute).apply(resultsqueue.append)

sqfit_out.apply(delayed(source_plotting.store_results),
                                           lines=[('sqx', 'sqy'), ('sqx', 'sqfit')],
                                           scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$", ylabel="I(q)")\
        .apply(client.compute)
sqphi_out.apply(delayed(source_plotting.store_results),
                                           images=['sqphi'], xlabel="$\phi$", ylabel="$q$", vmin=0, vmax=100)\
        .apply(client.compute).apply(resultsqueue.append)
sout_pca.apply(delayed(source_plotting.store_results),
                   images=['components']).apply(client.compute)\
                    .apply(resultsqueue.append)


# save to file system
sout_thumb.apply(delayed(source_file.store_results_file), {'writer' : 'npy', 'keys' : ['thumb']})\
        .apply(client.compute).apply(resultsqueue.append)
sout_circavg.apply(delayed(source_file.store_results_file), {'writer' : 'npy', 'keys' : ['sqx', 'sqy']})\
        .apply(client.compute).apply(resultsqueue.append)
#sout_circavg.apply(client.compute).apply(print)


# TODO : make databroker not save numpy arrays by default i flonger than a certain size 
# (since it's likely an error and leads to garbage strings saved in mongodb)
# save to databroker
'''
sout_thumb.apply(delayed(source_databroker.store_results_databroker), dbname="cms:analysis", external_writers={'thumb' : 'npy'})\
        .apply(client.compute).apply(resultsqueue.append)
'''




#origin.apply(compute).apply(lambda x : print("origin : {}".format(x)))

#sdoc = source_databroker.pullrecent("cms:data", start_time="2017-04-01", stop_time="2017-04-03")
#sdoc = delayed(sdoc)
#sdoc_gen = source_databroker.search("cms:data", start_time="2017-01-01", stop_time="2017-04-01", sample_savename="YT")

# User defined parameters
## searching through databroker
dbname_data = "cms:data"
#search_kws = {
#'start_time' : "2017-04-01",
##'sample_savename' : 'PM-EG2876_recast',
#'sample_savename' : 'PM-EG2884_recast',
    #}
search_kws = {
#'start_time' : "2017-05-01",
#'sample_savename' : 'YT',
'sample_name' : 'AgBH_Julien'
    }

data_uids = [
        'cdc07e39-c810-4fe7-b129-e769712d96f6',
        'f448325a-271e-418f-b99e-44d0b2dbb212',
        '25eeaf1c-c0e6-4feb-9d83-891e8ec385c3',
        '63c0e3e5-33a5-4559-96d5-7906a90677c6',
        'e6459f4f-eeaf-4c73-8ebf-2c50c30df01a',
        'a58f5674-a518-453b-bc80-d8b37afcdddd',
        '06d4a249-832f-4fb2-b5ef-6e76383551f1'
            ]

detector_key = 'pilatus300_image'
noqbins = None # If none, circavg will find optimal number

###### Done data loading ########
# read uids directly rather than search (search is slow, bypass for now)
#filename = os.path.expanduser("~/SciAnalysis-data/storage/YT_uids.txt")
# TODO : remove relative path from here
filename = os.path.expanduser("../storage/alldata-jan-april.txt")
data_uids = list()
with open(filename,"r") as f:
    for ln in f:
        ln = ln.strip("\n")
        data_uids.append(ln)


# Emitting data

#sdoc_gen = source_databroker.search(dbname_data, **search_kws)
data_uids = []
#sdoc_gen = source_databroker.pull(dbname_data, **search_kws)
sdoc_gen = source_databroker.pull(dbname_data, sample_name="AgBH_Julien")
for sdoc in sdoc_gen:
    data_uids.append(sdoc['attributes']['data_uid'])

#sdoc_gen = source_databroker.pullfromuids(dbname_data, data_uids)

from time import sleep
cnt = 0
#for sdoc in sdoc_gen:
for uid in data_uids:
    print("loading task for uid : {}".format(uid))

    ## TODO : emit should maybe transform according to wrapper?
    # add delayed wrapper
    #sdoc._wrapper = delayed
    #sin.emit(delayed(sdoc))
    # this is asynchronous, maybe add a sink that plots stats of the emitting?
    sin.emit(uid)
    # probably good idea not to bombard dask with computations
    # add a small interval between requests
    sleep(.5)


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


    Streams must never modify data, create a new copy everytime instead

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



# TODO merge check with get stitch


def norm_exposure(image=None, exposure_time=None, **kwargs):
    return image/exposure_time

def multiply(A, B):
    return A*B

def divide(A, B):
    return A/B


def safelog10(img):
    img_out = np.zeros_like(img)
    w = np.where(img > 0)
    img_out[w] = np.log10(img[w])
    return img_out
