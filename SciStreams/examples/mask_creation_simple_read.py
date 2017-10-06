# create the mask for the pilatus2M
# import stuff to make plotting easier (NOT advised to do for actual
# deployment)
import os.path
import numpy as np

from PIL import Image
from matplotlib.pyplot import ion, imshow, figure, clf, clim

# read some data via databroker
from SciStreams.interfaces.databroker.databases import databases

# this is a gui
from SciStreams.tools.MaskCreator import MaskCreator

from SciStreams.config import mask_config

from SciStreams.data.Mask import MasterMask, MaskGenerator
from SciStreams.detectors.mask_generators import generate_mask

# interactive mode
ion()

cmsdb = databases['cms:data']

# choose the detector key
detector_key = "pilatus2M_image"

# some optional filters to limit searches
cmsdb.add_filter(start_time="2017-09-13", stop_time="2017-09-14")
hdrs = (list(cmsdb(sample_name="AgBH_Sep13_2017_JH_test")))

imgs = cmsdb.get_images(hdrs, detector_key)

# 0 : GISAXS , 1 : SAXS
ind = 0
startdoc = hdrs[ind]['start']
img = imgs[ind]

''' As a test, we can try reading the mask '''
# now read
detector_key = 'pilatus2M_image'
fname = "~/research/projects/SciAnalysis-data"
fname = fname + "/masks/pilatus2M_image/mask_pilatus2M_master_1.npz"
filename = os.path.expanduser(fname)
# test that it loads fine
master_mask = MasterMask(filename)
blem_fname = mask_config[detector_key]['blemish']['filename']
blemish = np.array(Image.open(blem_fname))

mmg = MaskGenerator(master_mask, blemish)
mask = mmg.generate(-72.99992548, -65.00001532)

''' As a further test, we can test that the SciStreams library is also reading
it properly'''
# GISAXS
md = dict()
md.update(**startdoc)
md.update(detector_key=detector_key)

mask = generate_mask(**md)['mask']

# plot them to see they make sense
figure(2)
clf()
imshow(mask)
clim(0, 1)

figure(3)
clf()
imshow(img)
clim(0, 100)
