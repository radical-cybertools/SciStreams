'''
This test tests calibration object, but also tests that the same computation
is not recomputed

'''
from dask import cache
_cache = cache.Cache(1e9)
_cache.register()
#from distributed import Client
#_pipeline_client = Client("10.11.128.3:8786")

from dask import set_options
set_options(delayed_pure=True)

from SciAnalysis.analyses.XSAnalysis.Data import Calibration
from SciAnalysis.interfaces.databroker.databases import databases

# sources
from SciAnalysis.interfaces.databroker import databroker as source_databroker

start_time = "2017-03-04"
stop_time = "2017-04-05"
#scires_gen = source_databroker.pull("cms:data", start_time=start_time, stop_time=stop_time, measure_type="measure")
uid1 = "4f4f3c58-c3fb-489b-a653-dca7bae431fd"
uid2 = "30b669a6-37d6-4b34-9eb3-ed06c5f0e1c4"

scires_header = source_databroker.pullfromuid("cms:data", uid1)
calib = Calibration(scires_header['attributes'])

res = calib.calibration.compute()
#print(res)
resx = calib.qx_map.compute()
resy = calib.qy_map.compute()
resz = calib.qz_map.compute()
resr = calib.q_map.compute()

resangle = calib.angle_map.compute()

#header = dict(cddb[-3])
#print(header['start']['calibration_wavelength_A'])
#calib = Calibration(header)

scires_header = source_databroker.pullfromuid("cms:data", uid2)
calibold = calib
calib = Calibration(scires_header['attributes'])
resold = res
res = calib.calibration.compute()
#print(res)

print("Old and new are equivalent : {}".format(res == resold))
resx = calib.qx_map.compute()
resy = calib.qy_map.compute()
resz = calib.qz_map.compute()
resr = calib.q_map.compute()

resangle = calib.angle_map.compute()
