''' Make sure dask keys are the same for both calculations.'''

from dask import cache
_cache = cache.Cache(1e9)
_cache.register()

from SciAnalysis.analyses.XSAnalysis.Data import MasterMask, Mask,\
    MaskGenerator
from config import MASKDIR
from functools import wraps

master_mask_name = "pilatus300_mastermask.npz"
master_mask_filename = MASKDIR + "/" + master_mask_name
from interfaces.file.reading import FileDesc

master_mask = FileDesc(master_mask_filename).get().get()
master_mask_data = master_mask['master_mask']
x0, y0 = master_mask['x0_master'], master_mask['y0_master']
master_mask_origin = y0, x0


master_mask = MasterMask(master=master_mask_data, origin=master_mask_origin)

blemish = Mask(shape=(900,900))
mask_gen = MaskGenerator(master_mask, blemish)

submask = mask_gen.generate((300,800))
print(submask.key)
submask = submask.compute().get()

mask_gen = MaskGenerator(master_mask, blemish)
submask = mask_gen.generate((300,800))
print(submask.key)
submask = submask.compute().get()

from pylab import *
ion()
imshow(submask)
