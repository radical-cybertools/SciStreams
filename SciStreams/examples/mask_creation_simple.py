# create the mask for the pilatus2M
# import stuff to make plotting easier (NOT advised to do for actual
# deployment)
from pylab import *
ion()

# read some data via databroker
from SciStreams.interfaces.databroker.databases import databases
cmsdb = databases['cms:data']

# choose the detector key
detector_key = "pilatus2M_image"

# some optional filters to limit searches
cmsdb.add_filter(start_time="2017-09-13", stop_time="2017-09-14")
hdrs = (list(cmsdb(sample_name = "AgBH_Sep13_2017_JH_test")))

imgs = cmsdb.get_images(hdrs, detector_key)

# 0 : GISAXS , 1 : SAXS
ind = 0
startdoc = hdrs[ind]['start']
img = imgs[ind]

# this is a gui
from SciStreams.tools.MaskCreator import MaskCreator
msk = MaskCreator(data=img)
msk.set_clim(0,10)

msg = "Use the Mask GUI. Press Enter Here when done"
msg += " and code will continue"
input(msg)

mask = msk.mask

''' There are a few components to this:
    1. Get the detector positions during the creation of this mask
    2. Save the origin of detector in mask image. Here it's 0,0 since
        mask is the same shape. However, it can be other if the mask arises
        from a lager stitched image.
    3. Set the scl of the pixels (needed when transforming pixel to lab
        coordinates for detector)
    4. Determine what parameters define this mask. For example:
        bs_phi : the phi orientation of the beam stop. If this changed, then
        the mask is no longer valid
        bsx, bsy : beamstop x, y positions
        maybe positions of other obstructions?
        sample-detector distance : the shadow will change with these
    5. Finally, save all this into a npz file.

    The mask will then be loaded by a MasterMask object, which willl be loaded
    by a MaskGenerator object. The MaskGenerator object takes a y,x coordinate
    to generate a future mask. This y, x coordinate is the detector lab
    position.

'''
# now get detector position
refmotorx = startdoc['motor_SAXSx']
refmotory = startdoc['motor_SAXSy']

refpoint_lab = refmotory, refmotorx
# set this ref point to the 0, 0 coordinate
refpoint = 0,0
# pilatus pixel conversion
scl = .172, .172
import os.path
# prepare the filename
filename = os.path.expanduser("some_mask_name.npz")
kwargs = dict()
#kwargs.update(startdoc)
kwargs['mask'] = mask
kwargs['refpoint'] = refpoint
kwargs['refpoint_lab'] = refpoint_lab
kwargs['scl'] = scl
# the motor positions used to define the mask
kwargs['motor_bsphi'] = startdoc['motor_bsphi']
kwargs['motor_bsx'] = startdoc['motor_bsx']
kwargs['motor_bsy'] = startdoc['motor_bsy']
kwargs['detector_SAXS_distance_m'] = startdoc['detector_SAXS_distance_m']
# saving here (uncomment)
# saving is done hwere
np.savez(filename, **kwargs)

''' As a test, we can try reading the mask '''
# now read
detector_key = 'pilatus2M_image'
filename = os.path.expanduser("~/research/projects/SciAnalysis-data/masks/pilatus2M_image/mask_pilatus2M_master_1.npz")
# test that it loads fine
from SciStreams.data.Mask import MasterMask, MaskGenerator
master_mask = MasterMask(filename)
blem_fname = mask_config[detector_key]['blemish']['filename']
from PIL import Image
blemish = np.array(Image.open(blem_fname))

mmg = MaskGenerator(master_mask, blemish)
mask = mmg.generate([-72.99992548, -65.00001532])

''' As a further test, we can test that the SciStreams library is also reading
it properly'''
# GISAXS
from SciStreams.detectors.mask_generators import generate_mask
md = dict()
md.update(**startdoc)
md.update(detector_key=detector_key)

mask = generate_mask(**md)['mask']

# plot them to see they make sense
figure(2);clf()
imshow(mask);clim(0,1)

figure(3);clf()
imshow(img);clim(0,100)
