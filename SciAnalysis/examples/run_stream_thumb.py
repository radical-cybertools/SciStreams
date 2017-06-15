# test a XS run
import os

# dask imports
from dask import set_options, delayed, compute, persist
# assume all functions are pure globally
set_options(delayed_pure=False)

# misc imports
import sys
from config import MASKDIR
from metadatastore.core import NoEventDescriptors
import numpy as np



# SciAnalysis imports
## globals setup stuff
from SciAnalysis.globals import cache
from SciAnalysis.globals import client
## interfaces
from SciAnalysis.interfaces.StreamDoc import StreamDoc, parse_streamdoc, Stream
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.detectors import detectors2D
## Analyses
# the streams
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator
from SciAnalysis.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream

########## Initialise Data objects
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
# Pawel's image stitching
#search_kws = {
#'start_time' : "2017-04-01",
#'sample_savename' : 'PM-EG2876_recast',
#'sample_savename' : 'PM-EG2884_recast',
    #}

#YT's data
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

import matplotlib.pyplot as plt
#plt.ion()
#figs = np.arange(20,30)
#for fig in figs:
#   plt.figure(fig);plt.clf()

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
    exposure_time = attr['sample_exposure_time']
    print("got exposure time: {}".format(exposure_time))
    return exposure_time

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
    plt.clf();
    #plt.title("log10img")
    #plt.imshow(np.log10(img))
    plt.title("img")
    plt.imshow((img))
    plt.savefig(name)

    globaldict[name] = img
    #from PIL import Image
    #im = Image.fromarray(img)
    #im.save(name)

    #source_file.store_results_file(sdoc.select([(0,'image')]), {'writer' : 'npy', 'keys' : ['image']})

def save_plot_recent(sdoc, fignum=None, name=None):
    global globaldict
    # make sure it's not delayed
    if fignum is None:
        fignum = 20
    sdoc = compute(sdoc)[0]
    if name is None:
        name = 'recent.jpg'
    if len(sdoc['args']) == 2:
        args = sdoc['args'][0], sdoc['args'][1]
    else:
        args = sdoc['args'][0],

    plt.figure(fignum);
    plt.clf();
    plt.title("S(q)")
    plt.loglog(*args)
    plt.savefig(name)
    globaldict[name] = args
    #source_file.store_results_file(sdoc.select([(0,'sqx'), (1, 'sqy')]), {'writer' : 'npy', 'keys' : ['sqx', 'sqy']})


def splitfunc(sdoc):
    det_keys = sdoc['attributes']['detectors']
    sdoclist = list()
    for det in det_keys:
        newsdoc = StreamDoc(sdoc)
        newsdoc.add(attributes=dict(detector=det))
        sdoclist.append(newsdoc)
    return sdoclist

# Stream setup, datbroker data comes here (a header for now)
#input is first element in stream
sin = Stream(wrapper=delayed_wrapper)
# add some attributes after the fact, this is a patch
s2 = sin.add_attributes(detector_name=detector_key)
s2.apply(compute).apply(lambda x : print("printing {}".format(x[0]['attributes'])))
#sin.apply(compute)#.apply(lambda x : print(x[0]['attributes']['detector_name'])).apply(compute)

# SPLIT the stream for the various detectors
#  separate data from attributes
attributes = s2.apply(get_attributes)

# get image from the input stream
image = s2.select((detector_key,None))

# calibration setup
sin_calib, sout_calib = CalibrationStream(wrapper=delayed_wrapper)
calib_qmap = sout_calib['q_maps']
calibration = sout_calib['calibration']
origin = sout_calib['origin']

# connect attributes to sin_calib
attributes.apply(sin_calib.emit)
attributes.map(print_exposure).apply(compute)


# generate a mask
mskstr = origin.map(mmg.generate)
mask_stream = mskstr.select((0, 'mask'))
# compute it to test later
mask = mask_stream.apply(compute)

# circular average
sin_image_qmap = image.merge(calib_qmap, mask_stream)
sin_circavg, sout_circavg = CircularAverageStream(wrapper=delayed_wrapper)
sin_image_qmap.apply(sin_circavg.emit)

# image stitching
stitch = attributes.map(get_stitch).select((0, 'stitch'))
#stitch = attributes.map((lambda x, *args, **kwargs : (x['stitch'],))).select((0, 'stitch'))
exposure_time = attributes.map(get_exposure_time).select((0, 'exposure_time'))
#exposure_time = attributes.map(lambda x : x['sample_exposure_time']).select((0, 'exposure_time'))
exposure_mask = mask_stream.select(('mask', None))
exposure_mask = exposure_mask.merge(exposure_time.select(('exposure_time', None))).map(multiply)
exposure_mask = exposure_mask.select((0, 'mask'))

sin_imgstitch, sout_imgstitch = ImageStitchingStream(wrapper=delayed_wrapper)
img_masked = image.merge(mask_stream.select(('mask',None))).map(multiply)
img_mask_origin = img_masked.select((0,'image')).merge(exposure_mask.select(('mask','mask')), origin.select((0, 'origin')), stitch)
img_mask_origin.apply(sin_imgstitch.emit)

#thumbstream
sin_thumb, sout_thumb = ThumbStream(wrapper=delayed_wrapper, blur=1, resize=2)
image.apply(sin_thumb.emit)

sout_thumb.apply(client.compute).sink_to_deque()
sout_circavg.apply(client.compute).sink_to_deque()

from SciAnalysis.interfaces.databroker.databroker import store_results_databroker
def sink_databroker(sdoc, dbname, external_writers={}):
    store_results_databroker(compute(sdoc)[0], dbname, external_writers=external_writers)

from SciAnalysis.interfaces.file.file import store_results_file
def sink_file(sdoc, writers={}):
    ''' writers : [{'writer' : 'npy', 'keys' : ['sqx', 'sqy']}] for example
    '''
    store_results_file(compute(sdoc)[0], writers=writers)

# sinking to databroker and file
sout_thumb.apply(compute).apply(lambda x: x[0]).apply(sink_databroker, "cms:analysis", external_writers={'thumb' : 'npy'})
#sout_circavg.apply(sink_file, writers=[{'writer' : 'npy', 'keys' : ['image']}])

#sout_circavg.apply(sink_databroker, "cms:analysis", external_writers={'sqx' : 'npy', 'sqxerr' : 'npy', 'sqy' : 'npy', 'sqyerr' : 'npy'})
#sout_circavg.apply(sink_file, writers=[{'writer' : 'npy', 'keys' : ['sqx', 'sqxerr', 'sqy', 'sqyerr']}])
#sout_circavg.apply(sink_databroker, "cms:analysis", external_writers={'sqx' : 'npy', 'sqxerr' : 'npy', 'sqy' : 'npy', 'sqyerr' : 'npy'})

# PROBLEM : client.persist is not caching data when sent in stream
# I am not sure why


# some plotting/sinks
mask_stream.select(('mask', None)).apply(save_image_recent,fignum=21, name="mask.png").apply(compute)
image.apply(save_image_recent, fignum=22, name="image.png").apply(compute)
sout_imgstitch.select(('image', None)).apply(save_image_recent,fignum=23, name="stitched.png").apply(compute)
image.apply(save_image_recent, fignum=24, name="img_time.png").apply(compute)
sout_imgstitch.select(('mask', None)).apply(save_image_recent,fignum=25, name="stitch-mask.png").apply(compute)
sout_thumb.select(('thumb', None)).apply(save_image_recent,fignum=26, name="thumb.png").apply(compute)
#sout_thumb.apply(sink_mpl, 
#sout_circavg.select(('sqx', None), ('sqy', None)).apply(save_plot_recent, fignum=27, name="sq.png").apply(compute)
#exposure_mask.select(('mask', None)).apply(save_image_recent,fignum=28, name="exposure_mask.png").apply(compute)




#sdoc = source_databroker.pullrecent("cms:data", start_time="2017-04-01", stop_time="2017-04-03")
#sdoc = delayed(sdoc)
#sdoc_gen = source_databroker.search("cms:data", start_time="2017-01-01", stop_time="2017-04-01", sample_savename="YT")

# read uids directly rather than search (search is slow, bypass for now)
#filename = os.path.expanduser("~/SciAnalysis-data/storage/YT_uids.txt")
#uids = list()
#with open(filename,"r") as f:
    #for ln in f:
        #ln = ln.strip("\n")
        #uids.append(ln)


# Emitting data

#sdoc_gen = source_databroker.search(dbname_data, **search_kws)
sdoc_gen = source_databroker.pullfromuids(dbname_data, data_uids)

cnt = 0
import time
t1 = time.time()
for sdoc in sdoc_gen:
    if cnt == 0:
        sdoc['attributes']['stitch'] = 0
    else:
        sdoc['attributes']['stitch'] = 1

    cnt +=1

    ## TODO : emit should maybe transform according to wrapper?
    sin.emit(delayed(pure=True, name=sdoc.tokenize())(sdoc))

t2 = time.time()
print("elapsed : {}".format(t2-t1))

# plot results
from matplotlib.pyplot import clf, figure, imshow, plot

#from SciAnalysis.globals import tmp_cache
#img = tmp_cache.get("img_acc")
#mask = tmp_cache.get("mask_acc")
#plt.figure(30);
#plt.clf();
#plt.imshow(img)
#plt.figure(31);
#plt.clf();
#plt.imshow(mask)
#
#plt.figure(32);
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
        usually after applying compute, need to index 0th element
            compute(delfunc)[0]
        Maybe a dask error:
        %debug
        p dsk


    '''
