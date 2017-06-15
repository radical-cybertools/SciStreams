# test a XS run

# set up the distributed client
#from distributed import Client
#_pipeline_client = Client("10.11.128.3:8786")

from dask.cache import Cache
_cache = Cache(1e9)
_cache.register()

from dask import set_options
set_options(delayed_pure=True)

import sys

from config import MASKDIR

from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import reading as source_file
from SciAnalysis.interfaces.detectors import detectors2D

from SciAnalysis.analyses.XSAnalysis.Protocols import CircularAverage
from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, MaskGenerator, Calibration

from metadatastore.core import NoEventDescriptors


detector = detectors2D['pilatus300']

circavg = CircularAverage()

blemish_filename = MASKDIR + "/Pilatus300k_main_gaps-mask.png"
blemish = source_file.FileDesc(blemish_filename).get_raw()[:,:,0] > 1
blemish = blemish.astype(int)



# prepare master maskoimport 
import numpy as np
master_mask_name = "pilatus300_mastermask.npz"
master_mask_filename = MASKDIR + "/" + master_mask_name
res = np.load(master_mask_filename)
master_mask= res['master_mask']
x0, y0 = res['x0_master'], res['y0_master']
# rows, cols
origin = y0, x0


master_mask = MasterMask(master=master_mask, origin=origin)

mmg = MaskGenerator(master_mask, blemish)

# read in data
start_time = "2017-02-01"
stop_time = "2017-03-31"
scires_gen = source_databroker.pull("cms:data", start_time=start_time, stop_time=stop_time, experiment_type="TSAXS")

#uid = '0b2a6007-ab1f-4f09-99ad-74c1c0a4b391'
#scires_header = source_databroker.pullfromuid("cms:data", uid)
#def makegen(data):
    #yield data

#scires_gen = makegen(scires_header)

detector_key = 'pilatus300_image'
scires = scires_gen
cnt = 0
nobins = 1000

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

# save bad files after run, ignore when they rais exceptions
badfiles = list()

# transform generator to one element only 

from pylab import *
ion()

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
    except KeyError:
        continue
    origin = beamy0, beamx0

    mask = mmg.generate(origin)

    try:
        #scires_sq = circavg(image=img, calibration=calibration, mask=mask, bins=nobins).compute()
        scires_sq = circavg(image=img, q_map=calibration.q_map, r_map=calibration.r_map,  bins=None, mask=mask).compute()
        print("Read data set {}, with uid : {}".format(cnter, scires_sq['attributes']['data_uid']))
        sqx, sqy = scires_sq(['sqx', 'sqy']).get()
        figure(1);clf();
        loglog(sqx, sqy)
        pause(.1)

        img = img.get()
        figure(2);clf();
        imshow(img);clim(0,1e2)
        pause(.1)

    except Exception:
        badfiles.append(dict(uid=scires['attributes']['uid'], exception=sys.exc_info()[0], exception_details=sys.exc_info()[1]))
        print("Failed with exception: {}".format(sys.exc_info()[0]))
        continue
    


scires_sq = scires_sq.compute()
sqxerr, sqyerr = scires_sq(['sqxerr', 'sqyerr']).get()

img = img.get()

calib_params = calibration.calibration.get().compute()
x0, y0 = calib_params['beamx0']['value'], calib_params['beamy0']['value']

mask = mask.compute()
mask = mask.get()



figure(2);clf();
imshow(img);clim(0,1e3)
plot(x0, y0, 'ro')

figure(3);clf()
imshow(mask)
