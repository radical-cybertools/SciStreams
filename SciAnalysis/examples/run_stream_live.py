## test a XS run
import time
from time import sleep
import os, sys
from metadatastore.core import NoEventDescriptors
import numpy as np
import matplotlib
matplotlib.use("Agg")
from dask import delayed, compute
from collections import deque

# SciAnalysis imports
# this one does a bit of setup upon import, necessary
from SciAnalysis.globals import client, debugcache
import SciAnalysis.config as config

## interfaces
from SciAnalysis.interfaces.plotting_mpl import plotting_mpl as source_plotting
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml
from SciAnalysis.interfaces.detectors import detectors2D
# Streams include stuff
from SciAnalysis.interfaces.StreamDoc import StreamDoc, parse_streamdoc, Arguments
from SciAnalysis.interfaces.streams import Stream
## Analyses
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Obstruction
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream, QPHIMapStream
from SciAnalysis.analyses.XSAnalysis.CustomStreams import SqFitStream

# get databases (not necessary)
from SciAnalysis.interfaces.databroker.databases import databases
cddb = databases['cms:data']




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


def add_attributes(sdoc, **attr):
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc


# TODO merge check with get stitch
def check_stitchback(sdoc):
    if 'stitchback' not in sdoc['attributes']:
        sdoc['attributes']['stitchback'] = False
    return StreamDoc(sdoc)

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

# TODO :  need to fix this
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


from functools import partial
# Stream setup, datbroker data comes here (a string uid)
sin = Stream()
# TODO : run asynchronously 
s_event = sin.map(source_databroker.pullfromuid, dbname='cms:data',raw=True)#.filter(isSAXS)
s_event = s_event.map((check_stitchback),raw=True)

# next start working on result
s_event = s_event.map((add_attributes), stream_name="InputStream",raw=True)
s_event = s_event.map((set_detector_name), detector_name='pilatus300',raw=True)

#  separate data from attributes
attributes = s_event.map((lambda x: StreamDoc(args=x['attributes'])),raw=True)

# get image from the input stream
image = s_event.map(lambda x : (x.select)((detector_key,None)),raw=True).map(lambda x : x.astype(float))

# calibration setup
sin_calib, sout_calib = CalibrationStream()
# grab the origin
origin = sout_calib.map(lambda x : (x.origin[1], x.origin[0]))

# connect attributes to sin_calib
attributes.map(sin_calib.emit, raw=True)


# generate a mask
mskstr = origin.map(mmg.generate)

mask_stream = mskstr.select((0, 'mask'))

def pack(*args):
    return args

## circular average
sin_image_qmap = image.merge(sout_calib, mask_stream)
out_list = deque(maxlen=10)
sin_circavg, sout_circavg = CircularAverageStream()
sin_image_qmap.select(0, 1, 'mask').map(sin_circavg.emit, raw=True)

## image stitching
stitch = attributes.map(lambda x : x['stitchback']).select((0, 'stitchback'))
exposure_time = attributes\
        .map(lambda x : x['sample_exposure_time'])\
        .select((0, 'exposure_time'))

exposure_mask = mask_stream\
        .select(('mask', None))
exposure_mask = exposure_mask\
        .merge(exposure_time.select(('exposure_time', None)))\
        .map(lambda a,b: a*b)

exposure_mask = exposure_mask.select((0, 'mask'))

sin_imgstitch, sout_imgstitch = ImageStitchingStream()

sout_imgstitch_log = sout_imgstitch.select(('image', None))\
        .map(safelog10).select((0, 'image'))
sout_imgstitch_log = sout_imgstitch_log\
        .map(delayed(add_attributes), stream_name="ImgStitchLog", raw=True)

img_masked = image.merge(mask_stream.select(('mask',None))).map(lambda a,b : a*b)
img_mask_origin = img_masked.select((0,'image'))\
        .merge(exposure_mask.select(('mask','mask')), origin.select((0, 'origin')), stitch)
img_mask_origin.map(sin_imgstitch.emit, raw=True)

sin_thumb, sout_thumb = ThumbStream(blur=1, resize=2)
image.map(sin_thumb.emit, raw=True)
images = list()

sout_img_partitioned = sout_thumb.select(('thumb', None)).partition(100).map(squash, raw=True)

sout_img_pca = sout_img_partitioned.map(PCA_fit, n_components = 16).map(add_attributes, stream_name="PCA", raw=True).map(todict)

## fitting
#sqfit_in, sqfit_out = SqFitStream()
#sout_circavg.apply(sqfit_in.emit)

sqphi_in, sqphi_out = QPHIMapStream()
image.merge(mask_stream, origin.select((0, 'origin'))).map(sqphi_in.emit, raw=True)


## save to plots 
resultsqueue = deque(maxlen=1000)
sout_circavg.map((source_plotting.store_results), lines=[('sqx', 'sqy')],\
                   scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$",
                   ylabel="I(q)", raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_imgstitch.map((source_plotting.store_results), images=['image'], hideaxes=True, raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_thumb.map((source_plotting.store_results), images=['thumb'], hideaxes=True, raw=True).map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_thumb.select(('thumb', None)).map(safelog10).select((0,'thumb'))\
        .map((add_attributes), stream_name="ThumbLog", raw=True)\
        .map((source_plotting.store_results), images=['thumb'], hideaxes=True, raw=True)\
        .map(client.compute, raw=True).map(resultsqueue.append, raw=True)

sqphi_out.map(source_plotting.store_results,raw=True,
                                           images=['sqphi'], xlabel="$\phi$", ylabel="$q$", vmin=0, vmax=100)\
        .map(resultsqueue.append, raw=True)
sout_img_pca.map(source_plotting.store_results,
                   images=['components'], raw=True).map(client.compute, raw=True)\
                    .map(resultsqueue.append, raw=True)

## save to file system
sout_thumb.map((source_file.store_results_file), {'writer' : 'npy', 'keys' : ['thumb']}, raw=True)\
        .map(client.compute, raw=True).map(resultsqueue.append, raw=True)
sout_circavg.map((source_file.store_results_file), {'writer' : 'npy', 'keys' : ['sqx', 'sqy']}, raw=True)\
        .map(client.compute).map(resultsqueue.append, raw=True)

## save to xml
sout_circavg.map((source_xml.store_results_xml), outputs=None, raw=True)\
        .map(client.compute).map(resultsqueue.append, raw=True)

## TODO : make databroker not save numpy arrays by default i flonger than a certain size 
## (since it's likely an error and leads to garbage strings saved in mongodb)
## save to databroker
sout_circavg.map(source_databroker.store_results_databroker, dbname='cms:analysis',
                 external_writers={'sqx' : 'npy', 'sqy' : 'npy', 'sqxerr' :
                                   'npy', 'sqyerr' : 'npy'}, raw=True)



##### Now prepare data for the stream
# User defined parameters
## searching through databroker
dbname_data = "cms:data"
search_kws = {
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

#init_start_time = "2017-06-22"
#init_start_time = "2017-07-10"
init_start_time = "2017-07-11"


from SciAnalysis.interfaces.databroker.databases import databases
cddb = databases['cms:data']
start_time = init_start_time
skipfirst = True
while True:
    hdrs = cddb(start_time=start_time)
    hdrs = list(hdrs)
    hdrs.reverse()

    for hdr in hdrs:
        if skipfirst:
            skipfirst = False
            continue
        uid = hdr['start']['uid']

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
        except FileNotFoundError:
            print("File not found error for uid : {}".format(uid))
        except AttributeError:
            print("Attribute Error (probably the metadata is slightly different)")

    # now update the latest time
    last_time = hdr['start']['time']
    t1 = time.gmtime(time.ctime(last_time))
    start_time = time.strftime("%Y-%m-%d %H:%M", t1)
    # at the end, update time stamp for latest time
