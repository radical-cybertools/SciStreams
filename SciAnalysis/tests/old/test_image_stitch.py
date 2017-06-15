# test a XS run
'''
images to stitch
PM-EG2876_recast
PM-EG2884_recast

'''

# dask imports
# set up the distributed client
from dask.cache import Cache
_cache = Cache(1e9)
_cache.register()

from distributed import Client
client = Client("10.11.128.3:8786")

from dask import set_options
# assume all functions are pure globally
set_options(delayed_pure=True)

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
from SciAnalysis.analyses.XSAnalysis.Protocols import CircularAverage, Thumbnail
## Data
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Calibration

# Initialize Protocols
circavg = CircularAverage()
thumb = Thumbnail()

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
    #'sample_name' : 'PM-EG2876_recast',
     'sample_name' : 'PM-EG2884_recast',
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

# save bad files after run, ignore when they raise exceptions
badfiles = list()
from pylab import *
ion()

print("Searching database")
scires_gen = source_databroker.search(dbname_data, **search_kws)
print("Done, now computing results")

# return databroker friendly epoch to string
def _epoch2string(t1):
    return time.strftime("%Y-%M-%d %H:%m:%S",time.localtime(t1))

def xystitch(start_time=None, poisson=False, **kwargs):
    '''
        Stitch images with matching kwargs together
        history in seconds

        start_time : if negative, search back in time from now in seconds

        NOTE : It is assumed that the following keys are used for the stitch
        Poisson :  add Poisson noise again so image looks better (for stitched regions)
            only nice for figures
    '''
    import time
    from SciAnalysis.interfaces.detectors import detectors2D
    from SciAnalysis.interfaces.databroker.databroker import pull

    if start_time is not None and start_time < 0:
        start_time = _epoch2string(time.time() - start_time)

    scires_gen = pull("cms:data", start_time=start_time, **kwargs)

    exposure_time_key = "sample_exposure_time"
    x_shift_key = "motor_DETx"
    y_shift_key = "motor_DETy"
    detector_name = 'pilatus300'
    detector_info = detectors2D[detector_name]
    detector_key = detector_info['image_key']['value']
    detector_xpix_size = detector_info['pixel_size_x']['value']
    detector_ypix_size = detector_info['pixel_size_y']['value']

    scires_gen = pull("cms:data", start_time=start_time, **kwargs)
    img_acc, mask_acc, origin_acc = None, None, None
    # now stitch
    cnt = 0
    futures = list()
    for scires in scires_gen:
        print("reading image {}".format(cnt))
        cnt = cnt + 1
        img_tmp = scires(detector_key)
        img_tmp.addglobals(global_attrs)

        attributes = scires['attributes']
        motorx = attributes[x_shift_key]
        motory = attributes[x_shift_key]
        exposure_time = scires['attributes'][exposure_time_key]
        print("motor positions : {}, {} (x, y)".format(motorx, motory))
        calibration = Calibration(attributes)

        beamx0, beamy0 = calibration.calibration.get()['beamx0']['value'], calibration.calibration.get()['beamy0']['value']
        futures.append(client.persist(calibration.calibration, pure=True))
        # save intermediate computations
        futures.append(client.persist(calibration.q_map, pure=True))
        futures.append(client.persist(calibration.qx_map, pure=True))
        futures.append(client.persist(calibration.qy_map, pure=True))
        futures.append(client.persist(calibration.qx_map, pure=True))
        futures.append(client.persist(calibration.r_map, pure=True))
        beamx0, beamy0 = beamx0.compute(), beamy0.compute()
        print("beam center : {}, {}".format(beamx0, beamy0))

        img_next = img_tmp.get()/exposure_time
        origin_next = int(beamy0), int(beamx0)
        mask_next = mmg.generate(origin).compute().get()

        img_acc, mask_acc, origin_acc = xystitch_accumulate((img_acc, mask_acc, origin_acc), (img_next, mask_next, origin_next))

    img_acc = img_acc/mask_acc

    if poisson:
        w = np.where((mask_acc > 1)*(img_acc > 0))
        img_acc[w] = np.random.poisson(img_acc[w])
    mask_acc = mask_acc > 0

    return img_acc, mask_acc, origin_acc

def xystitch_accumulate(prevstate, newstate):
    '''

        (assumes IMG and mask np arrays)
        prevstate : IMG, mask, (x0, y0) triplet
        nextstate : incoming IMG, mask, (x0, y0) triplet

        returns accumulated state

        NOTE : x is cols, y is rows
            where img[rows][cols]
            so shapey, shapex = img.shape

        TODO : Do we want subpixel accuracy stitches?
            (this requires interpolating pixels, maybe not
                good for shot noise limited regime)
    '''
    # unraveling arguments
    img_acc, mask_acc, origin_acc = prevstate
    if img_acc is not None:
        shape_acc = img_acc.shape

    img_next, mask_next, origin_next = newstate
    shape_next = img_next.shape

    # logic for making new state
    # initialization:
    if img_acc is None:
        img_acc = img_next.copy()
        shape_acc = img_acc.shape
        mask_acc = mask_next.copy()
        origin_acc = origin_next
        return img_acc, mask_acc, origin_acc

    # logic for main iteration component
    # NOTE : In matplotlib, bottom and top are flipped (until plotting in
    # matrix convention), this is logically consistent here but not global
    bounds_acc = _getbounds2D(origin_acc, shape_acc)
    bounds_next = _getbounds2D(origin_next, shape_next)
    # check if image will fit in stitched image
    expandby = _getexpansion2D(bounds_acc, bounds_next)
    print("need to expand by {}".format(expandby))

    img_acc = _expand2D(img_acc, expandby)
    mask_acc = _expand2D(mask_acc, expandby)
    print("New shape : {}".format(img_acc.shape))

    origin_acc = origin_acc[0] + expandby[2], origin_acc[1] + expandby[0]
    _placeimg2D(img_next, origin_next, img_acc, origin_acc)
    _placeimg2D(mask_next, origin_next, mask_acc, origin_acc)

    return img_acc, mask_acc, origin_acc

def _placeimg2D(img_source, origin_source, img_dest, origin_dest):
    ''' place source image into dest image. use the origins for
    registration.'''
    bounds_image = _getbounds2D(origin_source, img_source.shape)
    left_bound = origin_dest[1] + bounds_image[0]
    low_bound = origin_dest[0] + bounds_image[2]
    img_dest[low_bound:low_bound+img_source.shape[0],
             left_bound:left_bound+img_source.shape[1]] += img_source

def _getbounds(center, width):
    return -center, width-1-center

def _getbounds2D(origin, shape):
    # NOTE : arrays index img[y][x] but I choose this way
    # because convention is in plotting, cols (x) is x axis
    yleft, yright = _getbounds(origin[0], shape[0])
    xleft, xright = _getbounds(origin[1], shape[1])
    return [xleft, xright, yleft, yright]

def _getexpansion(bounds_acc, bounds_next):
    expandby = [0, 0]
    # image accumulator does not extend far enough left
    if bounds_acc[0] > bounds_next[0]:
        expandby[0] = bounds_acc[0] - bounds_next[0]

    # image accumulator does not extend far enough right
    if bounds_acc[1] < bounds_next[1]:
        expandby[1] = bounds_next[1] - bounds_acc[1]

    return expandby

def _getexpansion2D(bounds_acc, bounds_next):
    expandby = list()
    expandby.extend(_getexpansion(bounds_acc[0:2], bounds_next[0:2]))
    expandby.extend(_getexpansion(bounds_acc[2:4], bounds_next[2:4]))
    return expandby

def _expand2D(img, expandby):
    ''' expand image by the expansion requirements. '''
    if not any(expandby):
        return img

    dcols = expandby[0] + expandby[1]
    drows = expandby[2] + expandby[3]

    img_tmp = np.zeros((img.shape[0] + drows, img.shape[1] + dcols))
    img_tmp[expandby[2]:expandby[2]+img.shape[0], expandby[0]:expandby[0]+img.shape[1]] = img

    return img_tmp



img_acc = None
mask_acc = None
origin_acc = None

img_next = np.ones((10,10))
mask_next = np.ones((10,10))
origin_next = (3,4)

img_acc, mask_acc, origin_acc = xystitch_accumulate((img_acc, mask_acc, origin_acc), (img_next, mask_next, origin_next))

origin_next = (3,8)
img_acc, mask_acc, origin_acc = xystitch_accumulate((img_acc, mask_acc, origin_acc), (img_next, mask_next, origin_next))

origin_next = (3,9)
img_acc, mask_acc, origin_acc = xystitch_accumulate((img_acc, mask_acc, origin_acc), (img_next, mask_next, origin_next))

origin_next = (2,8)
img_acc, mask_acc, origin_acc = xystitch_accumulate((img_acc, mask_acc, origin_acc), (img_next, mask_next, origin_next))

from pylab import *
rcParams['image.interpolation'] = None
figure(0);clf();
imshow(img_acc)
figure(1);clf();
imshow(mask_acc)
