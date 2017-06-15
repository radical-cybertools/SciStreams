## test a XS run
import os
#import matplotlib
#matplotlib.use("Agg")

# dask imports
# set up the distributed client
#from dask.cache import Cache
#_cache = Cache(1e9)
#_cache.register()

#from distributed import Client
#client = Client("10.11.128.3:8786")

from collections import deque

from SciAnalysis.globals import client, debugcache

from dask import set_options, delayed, compute
# assume all functions are pure globally
set_options(delayed_pure=True)

# misc imports
import sys
import SciAnalysis.config as config
from metadatastore.core import NoEventDescriptors
import numpy as np

# SciAnalysis imports
## interfaces
from SciAnalysis.interfaces.plotting_mpl import plotting_mpl as source_plotting
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml
from SciAnalysis.interfaces.detectors import detectors2D
## Analyses
#from SciAnalysis.analyses.XSAnalysis.Protocols_streams import CircularAverage, Thumbnail
## Data
# the stream version
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Obstruction
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream, QPHIMapStream
from SciAnalysis.analyses.XSAnalysis.CustomStreams import SqFitStream

from SciAnalysis.interfaces.databroker.databases import databases
cddb = databases['cms:data']


# Streams include stuff
# select needed for mapping
from SciAnalysis.interfaces.StreamDoc import StreamDoc, Stream, parse_streamdoc, Arguments
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


def print_cal(clb):
    print(clb)

from dask import compute

from functools import wraps

# This decorator is necessary to provide a way to find out what stream doc is
# unique. We need to tokenize only the args, kwargs and attributes of
# StreamDoc, nothing else
#from SciAnalysis.interfaces.StreamDoc import delayed_wrapper
#input_wrapper = delayed_wrapper

#def inspect_stream(stream):
    #stream.apply(compute).apply(lambda x : print(x[0]))

def get_attributes(sdoc):
    # reasoning behind this: need to compute the attributes before they're
    # evaluated in function, else it gets passed as delayed reference
    return StreamDoc(args=sdoc['attributes'])

def add_attributes(sdoc, **attr):
    # make a copy
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc

#import matplotlib.pyplot as plt
#plt.ion()
#plt.clf()

def get_stitchback(attr, *args, **kwargs):
    #print(attr['stitch'])
    #print("kwargs : {}".format(kwargs))
    #print("args : {}".format(args))
    return attr['stitchback']

# TODO merge check with get stitch
def check_stitchback(sdoc):
    if 'stitchback' not in sdoc['attributes']:
        sdoc['attributes']['stitchback'] = 0
    return StreamDoc(sdoc)

def get_exposure_time(attr, *args, **kwargs):
    return attr['sample_exposure_time']

def norm_exposure(image=None, exposure_time=None, **kwargs):
    return image.astype(float)/exposure_time

def multiply(A, B):
    return A*B

def divide(A, B):
    return A/B

def set_detector_name(sdoc, detector_name='pilatus300'):
    sdoc['attributes']['detector_name'] = detector_name
    return StreamDoc(sdoc)

def safelog10(img):
    img_out = np.zeros_like(img)
    w = np.where(img > 0)
    img_out[w] = np.log10(img[w])
    return img_out

def todict(arg):
    return Arguments(**arg)

def toargs(arg):
    return Arguments(*arg)

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

# TODO :  need to fix this
#@delaye
def squash(sdocs):
    newsdoc = StreamDoc()
    for sdoc in sdocs:
        newsdoc.add(attributes = sdoc['attributes'])
    N = len(sdocs)
    cnt = 0
    newargs = []
    newkwargs = dict()
    for sdoc in sdocs:
        args, kwargs = sdoc['args'], sdoc['kwargs']
        for i, arg in enumerate(args):
            if cnt == 0:
                if isinstance(arg, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(arg.shape)
                    newargs.append(np.zeros(newshape))
                else:
                    newargs.append([])
            if isinstance(arg, np.ndarray):
                newargs[i][cnt] = arg
            else:
                newargs[i].append[arg]

        for key, val in kwargs.items():
            if cnt == 0:
                if isinstance(val, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(val.shape)
                    newkwargs[key] = np.zeros(newshape)
                else:
                    newkwargs[key] = []
            if isinstance(val, np.ndarray):
                newkwargs[key][cnt] = val
            else:
                newkwargs[key].append[val]

        cnt = cnt + 1

    newsdoc.add(args=newargs, kwargs=newkwargs)

    return newsdoc

def isSAXS(sdoc):
    ''' return true only if a SAXS expt.'''
    attr = sdoc['attributes']
    if 'experiment_type' in attr:
        #print("experiment type : {}".format(attr['experiment_type']))
        expttype = attr['experiment_type']
        if expttype == 'SAXS' or expttype == 'TSAXS':
            return True
    return False



globaldict = dict()

import dask
def compute(obj):
    ''' slight modification to remove this tuple being returned.'''
    return dask.compute(obj)[0]

from functools import partial
# Stream setup, datbroker data comes here (a header for now)
sin = Stream()
# first expects a string, just apply (ignore wrappers)
# this can't be delayed because filter contains an if statement
# TODO : run asynchronously 
# (maybe clients spawn other clients eventually)
# i.e. : control loops should be run elsewhere from the data uid fetching
# alternative, only pass metadata without loading image, may be more practical and faster
s_event = sin.map(source_databroker.pullfromuid, dbname='cms:data',raw=True).map(compute,raw=True).filter(isSAXS)#.map(delayed,raw=True)
s_event = s_event.map((check_stitchback),raw=True)
#s2.apply(compute).apply(print)
# next start working on result
s_event = s_event.map((add_attributes), stream_name="InputStream",raw=True)#.apply(compute)
#s_event.apply(client.compute).apply(print)
s_event = s_event.map((set_detector_name), detector_name='pilatus300',raw=True)
#sin.map(print, raw=True)
#s2.apply(compute).apply(lambda x : print(x[0]['attributes']['stream_name']))

#  separate data from attributes
attributes = s_event.map((get_attributes),raw=True)
#attributes.map(compute, raw=True).map(print, raw=True)

# get image from the input stream
#image = s2.select((detector_key,None))
#s_event.apply(compute).apply(print)
image = s_event.map(lambda x : (x.select)((detector_key,None)),raw=True).map(lambda x : x.astype(float))

# calibration setup
sin_calib, sout_calib = CalibrationStream()
#sin_calib.map(compute, raw=True).map(lambda x : x['args'],raw=True).map(print, raw=True)
#sin_calib.apply(print)
#sout_calib['q_maps'].apply(compute).apply(print)
# old
#calib_qmap = sout_calib['q_maps']
#calibration = sout_calib['calibration']
#origin = sout_calib['origin']
#sout_calib.apply(compute).map(print)
origin = sout_calib.map(lambda x : (x.origin[1], x.origin[0]))
origin.map(compute, raw=True)
#calib_qmap.map(compute, raw=True)
#calibration.map(compute, raw=True).map(print, raw=True)

# connect attributes to sin_calib
attributes.map(sin_calib.emit, raw=True)#.apply(compute)
attributes.map(compute, raw=True)
##attributes.map(print_exposure).apply(compute)


# generate a mask
mskstr = origin.map(mmg.generate)

mask_stream = mskstr.select((0, 'mask'))

#mask_list = deque(maxlen=10)
#img_list = deque(maxlen=10)
#origin_list = deque(maxlen=10)
#image.map(img_list.append).apply(compute)
#mask_stream.select(('mask', None)).map(mask_list.append).apply(compute)
#origin.map(origin_list.append).apply(compute)
# compute it to test later
# TODO add when dask
#mask = mask_stream.(compute)
def pack(*args):
    return args

## circular average
sin_image_qmap = image.merge(sout_calib, mask_stream)
out_list = deque(maxlen=10)
sin_circavg, sout_circavg = CircularAverageStream()
sin_image_qmap.select(0, 1, 'mask').map(sin_circavg.emit, raw=True)
#sout_circavg.apply(compute).apply(print)
#sout_circavg.select(('sqx',None),('sqy', None)).map(pack).map(out_list.append).apply(compute)

# image stitching
stitch = attributes.map(get_stitchback).select((0, 'stitchback'))
exposure_time = attributes.map(get_exposure_time).select((0, 'exposure_time'))
exposure_mask = mask_stream.select(('mask', None))
exposure_mask = exposure_mask.merge(exposure_time.select(('exposure_time', None))).map(multiply)
exposure_mask = exposure_mask.select((0, 'mask'))

sin_imgstitch, sout_imgstitch = ImageStitchingStream()
#sout_imgstitch.map(print, raw=True).map(compute, raw=True)
#sout_imgstitch_log = sout_imgstitch.select(('image', None)).map(safelog10).select((0, 'image'))
#sout_imgstitch_log = sout_imgstitch_log.map(delayed(add_attributes), stream_name="ImgStitchLog", raw=True)
img_masked = image.merge(mask_stream.select(('mask',None))).map(multiply)
img_mask_origin = img_masked.select((0,'image')).merge(exposure_mask.select(('mask','mask')), origin.select((0, 'origin')), stitch)
img_mask_origin.map(sin_imgstitch.emit, raw=True)
#sin_imgstitch.apply(compute).apply(lambda x: print("printing : {}".format(x)))

sin_thumb, sout_thumb = ThumbStream(blur=1, resize=2)
image.map(sin_thumb.emit, raw=True)
images = list()
image.map(compute, raw=True).map(images.append)
#
#
#
sout_img_partitioned = sout_thumb.select(('thumb', None)).partition(100).map(squash, raw=True)
##sout_img_partitioned.apply(compute).apply(print)
## saves dict with components key
sout_img_pca = sout_img_partitioned.map(PCA_fit, n_components = 16).map(add_attributes, stream_name="PCA", raw=True).map(todict)
#sout_img_pca.map(lambda x : print("pca sdoc : {}".format(x)), raw=True)
#
#
## fitting
#sqfit_in, sqfit_out = SqFitStream()
#sout_circavg.apply(sqfit_in.emit)
#
#
##
sqphis = deque(maxlen=10)
sqphi_in, sqphi_out = QPHIMapStream()
image.merge(mask_stream, origin.select((0, 'origin'))).map(sqphi_in.emit, raw=True)
sqphi_out.map(sqphis.append, raw=True)
#
#'''
#
##sout_thumb.apply(client.compute).sink_to_deque()
#sout_circavg.apply(compute)
#sout_imgstitch.apply(compute)
#sout_thumb.apply(compute)
#'''
#
# save to plots 
resultsqueue = deque(maxlen=1000)
sout_circavg.map((source_plotting.store_results), lines=[('sqx', 'sqy')],\
                   scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$",
                   ylabel="I(q)", raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_imgstitch.map((source_plotting.store_results), images=['image'], hideaxes=True, raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
#sout_imgstitch_log.map((source_plotting.store_results), images=['image'], hideaxes=True, raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_thumb.map((source_plotting.store_results), images=['thumb'], hideaxes=True, raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_thumb.select(('thumb', None)).map(safelog10).select((0,'thumb'))\
        .map((add_attributes), stream_name="ThumbLog", raw=True)\
        .map((source_plotting.store_results), images=['thumb'], hideaxes=True, raw=True)\
        .map(client.compute, raw=True).map(resultsqueue.append, raw=True)

#tmpqueue = deque(maxlen=10)
#sqfit_out.apply(client.compute).apply(tmpqueue.append)
#
#sqfit_out.apply(delayed(source_plotting.store_results),
#                                           lines=[('sqx', 'sqy'), ('sqx', 'sqfit')],
#                                           scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$", ylabel="I(q)")\
#        .apply(client.compute)
sqphi_out.map(source_plotting.store_results,raw=True,
                                           images=['sqphi'], xlabel="$\phi$", ylabel="$q$", vmin=0, vmax=100)\
        .map(resultsqueue.append, raw=True)
sout_img_pca.map(source_plotting.store_results,
                   images=['components'], raw=True).map(client.compute, raw=True)\
                    .map(resultsqueue.append, raw=True)
#
#
## save to file system
sout_thumb.map((source_file.store_results_file), {'writer' : 'npy', 'keys' : ['thumb']}, raw=True)\
        .map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_circavg.map((source_file.store_results_file), {'writer' : 'npy', 'keys' : ['sqx', 'sqy']}, raw=True)\
        .map(client.compute).map(resultsqueue.append, raw=True)
#sout_circavg.apply(client.compute).apply(print)
#
#
# save to xml
sout_circavg.map((source_xml.store_results_xml), outputs=None, raw=True)\
        .map(client.compute).map(resultsqueue.append, raw=True)
## TODO : make databroker not save numpy arrays by default i flonger than a certain size 
## (since it's likely an error and leads to garbage strings saved in mongodb)
## save to databroker
#'''
#sout_thumb.apply(delayed(source_databroker.store_results_databroker), dbname="cms:analysis", external_writers={'thumb' : 'npy'})\
#        .apply(client.compute).apply(resultsqueue.append)
#
#
#'''
#
#
##origin.apply(compute).apply(lambda x : print("origin : {}".format(x)))
#
##sdoc = source_databroker.pullrecent("cms:data", start_time="2017-04-01", stop_time="2017-04-03")
##sdoc = delayed(sdoc)
##sdoc_gen = source_databroker.search("cms:data", start_time="2017-01-01", stop_time="2017-04-01", sample_savename="YT")
#
## read uids directly rather than search (search is slow, bypass for now)






import os
#filename = os.path.expanduser("~/SciAnalysis-data/storage/YT_uids.txt")
# TODO : remove relative path from here
filename = os.path.expanduser("../storage/alldata-jan-april.txt")
data_uids = list()
with open(filename,"r") as f:
    for ln in f:
        ln = ln.strip("\n")
        data_uids.append(ln)


# Emitting data

''' AgBH_Julien
#sdoc_gen = source_databroker.search(dbname_data, **search_kws)
data_uids = []
#sdoc_gen = source_databroker.pull(dbname_data, **search_kws)
sdoc_gen = source_databroker.pull(dbname_data, sample_name="AgBH_Julien")
for sdoc in sdoc_gen:
    data_uids.append(sdoc['attributes']['data_uid'])
'''

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
    try:
        sin.emit(uid)
        # probably good idea not to bombard dask with computations
        # add a small interval between requests
        sleep(.1)
    except KeyError:
        print("Got a keyerror (no image likely), ignoring")
    except ValueError as e:
        print("got ValueError: {}".format(e))
