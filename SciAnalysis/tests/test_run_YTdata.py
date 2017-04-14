# test a XS run

# dask imports
# set up the distributed client
from dask.cache import Cache
_cache = Cache(1e9)
_cache.register()

from distributed import Client
client = Client("10.11.128.3:8786")

from dask import set_options
set_options(delayed_pure=True)

# misc imports
import sys
from config import MASKDIR
from metadatastore.core import NoEventDescriptors
import numpy as np

# SciAnalysis imports
## interfaces
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import reading as source_file
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
'sample_savename' : 'YT',
    }
detector_key = 'pilatus300_image'
noqbins = None # If none, circavg will find optimal number

# some global attributes to inherit
global_attrs = [
        "sample_name",
        "sample_savename",
        "data_uid",
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

scires_gen = source_databroker.search(dbname_data, **search_kws)

futures = list()
cnter = 0
while True:
    cnter += 1
    print("Trying number {}".format(cnter))
    try:
        scires = next(scires_gen)
    except StopIteration:
        break
    except NoEventDescriptors:
        # TODO : put uid of file
        badfiles.append("unknown")

    # prepare the sciresult
    img = scires(detector_key)
    # add the global attributes
    img.addglobals(global_attrs)

    # do not add attributes to calibration object, we want it reused by other data sets
    attributes = scires['attributes']
    try:
        calibration = Calibration(attributes)
        beamx0, beamy0 = calibration.calibration.get()['beamx0']['value'], calibration.calibration.get()['beamy0']['value']
        futures.append(client.persist(calibration.calibration, pure=True))
        # save intermediate computations
        futures.append(client.persist(calibration.q_map, pure=True))
        futures.append(client.persist(calibration.qx_map, pure=True))
        futures.append(client.persist(calibration.qy_map, pure=True))
        futures.append(client.persist(calibration.qx_map, pure=True))
        futures.append(client.persist(calibration.r_map, pure=True))
    except KeyError:
        continue
    origin = beamy0, beamx0

    mask = mmg.generate(origin)

    try:
        scires_sq = circavg(image=img, q_map=calibration.q_map, r_map=calibration.r_map,  bins=None, mask=mask)
        scires_thumb = thumb(image=img, blur=.1, crop=None, resize=None, vmin=None, type='log', vmax=None, mask=mask)
        futures.append(client.persist(scires_sq, pure=True))
        futures.append(client.persist(scires_thumb, pure=True))
        print("Read data set {}, with uid : {}".format(cnter, scires['attributes']['data_uid']))
        print("Group : {}".format(scires['attributes']['experiment_group']))
        print("Save name: {}".format(scires['attributes']['sample_savename']))

        #sqx, sqy = scires_sq(['sqx', 'sqy']).get()
        #figure(1);clf();
        #loglog(sqx, sqy)
        #pause(.1)

        #img = img.get()
        #figure(2);clf();
        #imshow(img);clim(0,1e2)
        #pause(.1)

    except FileNotFoundError:
        badfiles.append(dict(uid=scires['attributes']['uid'], exception=sys.exc_info()[0], exception_details=sys.exc_info()[1]))
        print("Failed with exception: {}".format(sys.exc_info()[0]))
        continue



