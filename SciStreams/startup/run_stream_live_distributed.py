# test a XS run
import time
from time import sleep
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")  # noqa
# from dask import delayed, compute
from collections import deque

# SciStreams imports
# this one does a bit of setup upon import, necessary
from SciStreams.globals import client
import SciStreams.config as config

# interfaces
from SciStreams.interfaces.plotting_mpl import plotting_mpl as iplotting
from SciStreams.interfaces.databroker import databroker as idb
from SciStreams.interfaces.file import file as ifile
from SciStreams.interfaces.xml import xml as ixml
# from SciStreams.interfaces.detectors import detectors2D
# Streams include stuff
from SciStreams.interfaces.StreamDoc import StreamDoc, Arguments

# wrappers for parsing streamdocs
from SciStreams.interfaces.StreamDoc import psdm, psda, squash

from SciStreams.interfaces.streams import Stream
# Analyses
from SciStreams.analyses.XSAnalysis.Data import \
        MasterMask, MaskGenerator, Obstruction
from SciStreams.analyses.XSAnalysis.Streams import CalibrationStream,\
    CircularAverageStream, ImageStitchingStream, ThumbStream, QPHIMapStream
# from SciStreams.analyses.XSAnalysis.CustomStreams import SqFitStream

# wrappers for parsing streamdocs
from SciStreams.interfaces.StreamDoc import select, pack, unpack, todict, toargs,\
        add_attributes, psdm, psda, merge

from distributed.utils import sync
from tornado.ioloop import IOLoop
from tornado import gen

# get databases (not necessary)
from SciStreams.interfaces.databroker.databases import databases
detector_key = "pilatus300_image"



def search_mask(detector_key, date=None):
    ''' search for a mask in the mask dir using the detectorkey
        optionally use date to search for a mask for a specific date.

        assumes detector_key is of form
            detectorname_image
            (underscore separating detector name and other attribute of it)

    '''
    # TODO : implement for specific date
    mask_dir = config.maskdir + "/" + detector_key
    # search using date NOT IMPLEMENTED
    if date is not None:
        raise NotImplementedError

    # strip the last part i.e. "_image"
    searchstring = detector_key[:detector_key.rfind("_")]

    matching_files = list()
    for dirpath, dirnames, filenames in os.walk(mask_dir):
        matching_files.extend([mask_dir + "/" + filename
                               for filename in filenames
                               if searchstring in filename])

    if len(matching_files) == 0:
        errormsg = "Error, could not find a "
        errormsg += "file matching {}\n".format(searchstring)
        errormsg += "mask dir : {}".format(mask_dir)
        raise ValueError(errormsg)

    matching_file = matching_files[0]

    return matching_file


# Initialise Data objects
# Blemish file
# blemish_filename = config.maskdir + "/Pilatus300k_main_gaps-mask.png"
blemish_filename = search_mask(detector_key)
blemish = ifile.FileDesc(blemish_filename).get_raw()[:, :, 0] > 1
blemish = blemish.astype(int)
# prepare master mask import
SAXS_bstop_fname = "pilatus300_mastermask.npz"
res = np.load(config.maskdir + "/" + SAXS_bstop_fname)
SAXS_bstop_mask = res['master_mask']
SAXS_bstop_origin = res['y0_master'], res['x0_master']
obs_SAXS = Obstruction(SAXS_bstop_mask, SAXS_bstop_origin)

GISAXS_bstop_fname = "mask_master_CMS_GISAXS_May2017.npz"
res = np.load(config.maskdir + "/" + GISAXS_bstop_fname)
GISAXS_bstop_mask = res['mask']
GISAXS_bstop_origin = res['origin']
obs_GISAXS = Obstruction(GISAXS_bstop_mask, GISAXS_bstop_origin)

obs_total = obs_GISAXS + obs_SAXS
obs3 = obs_total - obs_SAXS


# rows, cols
# origin = y0, x0
# Instantiate the MasterMask
master_mask = MasterMask(master=obs_total.mask, origin=obs_total.origin)
# Instantiate the master mask generator
mmg = MaskGenerator(master_mask, blemish)

# TODO merge check with get stitch
def check_stitchback(sdoc):
    sdoc['attributes']['stitchback'] = True
    # if 'stitchback' not in sdoc['attributes']:
    # sdoc['attributes']['stitchback'] = False
    return StreamDoc(sdoc)


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


globaldict = dict()

# Stream setup, datbroker data comes here (a string uid)
sin = Stream()
# TODO : run asynchronously?

s_event = sin\
        .map(idb.pullfromuid, dbname='cms:data')

s_event = s_event.map(check_stitchback)

# next start working on result
s_event = s_event\
        .map(add_attributes, stream_name="InputStream")
s_event = s_event\
        .map(set_detector_name, detector_name='pilatus300')

#  separate data from attributes
attributes = s_event.map((lambda x: StreamDoc(args=x['attributes'])))

# get image from the input stream
image = s_event.map(lambda x: (x.select)((detector_key, None)))\
        .map(psdm(lambda x: x.astype(float)))

# calibration setup
sin_calib, sout_calib = CalibrationStream()
# grab the origin
origin = sout_calib.map(psdm(lambda x: (x.origin[1], x.origin[0])))

# connect attributes to sin_calib
attributes.map(sin_calib.emit)


# example on how to quickly blemish a pixel
def blemish_mask(mask):
    # Add more entries here to blemish extra pixels
    mask[248, 56] = 0
    return mask

# generate a mask
mskstr = origin.map(psdm(mmg.generate))
mskstr = mskstr.map(psdm(blemish_mask))

mask_stream = mskstr.map(select, (0, 'mask'))


# circular average
sin_image_qmap = image.zip(sout_calib, mask_stream).map(merge)
out_list = deque(maxlen=10)
sin_circavg, sout_circavg = CircularAverageStream()
sin_image_qmap.map(select, 0, 1, 'mask').map(sin_circavg.emit)

# image stitching
stitch = attributes\
        .map(psdm(lambda x: x['stitchback'])).map(select, (0, 'stitchback'))
exposure_time = attributes\
        .map(psdm(lambda x: x['sample_exposure_time']))\
        .map(select, (0, 'exposure_time'))

exposure_mask = mask_stream\
        .map(select, ('mask', None))
exposure_mask = exposure_mask\
        .zip(exposure_time.map(select,('exposure_time', None))).map(merge)\
        .map(psdm(lambda a, b: a*b))

exposure_mask = exposure_mask.map(select, (0, 'mask'))

# set return_intermediate to True to get all stitches
sin_imgstitch, sout_imgstitch = ImageStitchingStream(return_intermediate=False)

sout_imgstitch_log = sout_imgstitch.map(select,('image', None))\
        .map(psdm(safelog10)).map(select, (0, 'image'))
sout_imgstitch_log = sout_imgstitch_log\
        .map(add_attributes, stream_name="ImgStitchLog")

img_masked = image\
        .zip(mask_stream.map(select, ('mask', None))).map(merge).map(psdm(lambda a, b: a*b))
img_mask_origin = img_masked.map(select, (0, 'image'))\
        .zip(exposure_mask.map(select, ('mask', 'mask')),
             origin.map(select, (0, 'origin')),
             stitch).map(merge)
img_mask_origin.map(sin_imgstitch.emit)

sin_thumb, sout_thumb = ThumbStream(blur=1, resize=2)
image.map(sin_thumb.emit)
images = list()

sout_img_partitioned = sout_thumb.map(select, ('thumb', None))\
        .partition(100)\
        .map(squash)

sout_img_pca = sout_img_partitioned\
        .map(psdm(PCA_fit), n_components=16)\
        .map(add_attributes, stream_name="PCA").map(psdm(todict))

# fitting
# sqfit_in, sqfit_out = SqFitStream()
# sout_circavg.apply(sqfit_in.emit)

sqphi_in, sqphi_out = QPHIMapStream()
image.zip(mask_stream, origin.map(select, (0, 'origin'))).map(merge)\
        .map(sqphi_in.emit)


# save to plots
resultsqueue = deque(maxlen=1000)
sout_circavg.map((iplotting.store_results),
                 lines=[('sqx', 'sqy')],
                 scale='loglog', xlabel="$q\,(\mathrm{\AA}^{-1})$",
                 ylabel="I(q)")\
        #.map(client.compute).map(resultsqueue.append)
sout_imgstitch\
        .map((iplotting.store_results),
             images=['image'], hideaxes=True)\
        #.map(client.compute).map(resultsqueue.append)

sout_imgstitch_log\
        .map((iplotting.store_results), images=['image'],
             hideaxes=True)\
        #.map(client.compute)\
        #.map(resultsqueue.append)
sout_thumb\
        .map((iplotting.store_results), images=['thumb'],
             hideaxes=True)\
        #.map(client.compute)\
        #.map(resultsqueue.append)
sout_thumb.map(select, ('thumb', None)).map(psdm(safelog10)).map(select, (0, 'thumb'))\
        .map(add_attributes, stream_name="ThumbLog")\
        .map(iplotting.store_results, images=['thumb'],
             hideaxes=True)\
        #.map(client.compute).map(resultsqueue.append)

sqphi_out.map(iplotting.store_results,
              images=['sqphi'], xlabel="$\phi$",
              ylabel="$q$", vmin=0, vmax=100)\
        #.map(resultsqueue.append)
sout_img_pca\
        .map(iplotting.store_results,
             images=['components'])\
        #.map(client.compute)\
        #.map(resultsqueue.append)

# save to file system
sout_thumb\
        .map((ifile.store_results_file),
             {'writer': 'npy', 'keys': ['thumb']})\
        #.map(client.compute).map(resultsqueue.append)
sout_circavg\
        .map((ifile.store_results_file),
             {'writer': 'npy', 'keys': ['sqx', 'sqy']})\
        #.map(client.compute).map(resultsqueue.append)

# save to xml
sout_circavg.map((ixml.store_results_xml), outputs=None)\
        #.map(client.compute).map(resultsqueue.append)

# TODO : make databroker not save numpy arrays by default i flonger than a
# certain size
# sample databroker save (not implemented)
# sout_circavg.map(idb.store_results_databroker,
# dbname='cms:analysis', external_writers={'sqx' : 'npy', 'sqy' : 'npy',
# 'sqxerr' : 'npy', 'sqyerr' : 'npy'}, raw=True)


# Now prepare data for the stream
# parameters

# Done data loading ########


# Emitting data

@gen.coroutine
def start_run_loop(start_time, dbname="cms:data",
              noqbins=None):
    ''' Start a live run of pipeline.'''
    print("running loop")
    last_uid = None
    cddb = databases[dbname]
    while True:
        print("running loop")
        hdrs = cddb(start_time=start_time)
        # need to reverse the headers in the correct order
        hdrs = list(hdrs)
        hdrs.reverse()

        for hdr in hdrs:
            uid = hdr['start']['uid']

            # always skip the last uid
            if uid == last_uid:
                continue

            print("Loading task for uid : {}".format(uid))

            try:
                yield sin.emit(uid)
                sleep(.1)
            except KeyError as e:
                errormsg = "Got a keyerror (no image likely), ignoring\n"
                errormsg += "{}".format(e)
            except ValueError as e:
                print("got ValueError: {}".format(e))
            except FileNotFoundError:
                print("File not found error for uid : {}".format(uid))
            except AttributeError:
                print("Attribute Error (probably the " +
                      "metadata is slightly different)")

        # get the latest time, add 1 second to not overlap it
        last_uid = hdr['start']['uid']
        last_time = hdr['start']['time']
        t1 = time.localtime(last_time)
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", t1)
        # at the end, update time stamp for latest time
        print("Reached end, waiting 1 sec for more data...")
        sleep(1)

def start_run(start_time, dbname="cms:data", noqbins=None):
    loop = IOLoop()
    sync(loop, start_run_loop, start_time, dbname=dbname, noqbins=noqbins)

if __name__ == "__main__":
    start_time = time.time()-24*3600
    start_run(start_time)
