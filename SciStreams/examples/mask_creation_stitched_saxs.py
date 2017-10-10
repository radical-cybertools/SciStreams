# make the mask for cms
# first load the data for cms
# now making the CMS mask
import numpy as np
from matplotlib.pyplot import ion, figure, clf, clim, imshow, pause, plot

from SciStreams.interfaces.databroker.databases import databases
from SciStreams.config import config

from SciStreams.tools.MaskCreator import MaskCreator

# need this to work with stitched images
from SciStreams.data.StitchedImage import StitchedImage
from PIL import Image

cmsdb = databases['cms:data']
ion()

# CMS stuff
det_key = "pilatus300_image"

maskdir = config['maskdir'] + "/" + det_key

blemish = np.array(Image.open(maskdir + "/pilatus300_mask_main.png"))
mask = blemish


hdrs = list(cmsdb(sample_name="AgBH_Julien"))
# need to reverse the list! :-(
hdrs.reverse()

hdrs = hdrs[:15]
Nhdrs = len(hdrs)
# for pilatus300
pixel_scl = .172, .172
# stitch images together, keeping a common reference point
for i, hdr in enumerate(hdrs):
    print("stitching {} of {}".format(i, Nhdrs))
    # hdr = hdrs[1]
    md = hdr['start']
    # image, mask, origin, stitchback
    # assume we stitch all images
    img = cmsdb.get_images(hdr, det_key)[0]
    img = img/md['sample_exposure_time']
    # if first time, first make ref position
    if i == 0:
        # set reference position
        detector_refx, detector_refy = md['motor_SAXSx'], md['motor_SAXSy']
        refpoint = 0, 0
        simg = StitchedImage(img, refpoint)
        simg_mask = StitchedImage(blemish, refpoint)
    else:
        detectory, detectorx = md['motor_SAXSy'], md['motor_SAXSx']
        shifty, shiftx = detectory-detector_refy, detectorx-detector_refx
        # has to be y, x in general
        shifty_pix, shiftx_pix = shifty/pixel_scl[0], shiftx/pixel_scl[1]
        refpoint = shifty_pix, shiftx_pix

        simg = simg + StitchedImage(img, refpoint)
        simg_mask = simg_mask + StitchedImage(mask, refpoint)

    figure(2)
    clf()
    imshow(simg.image/simg_mask.image)
    plot(simg.refpoint[1], simg.refpoint[0], 'ro')
    clim(0, 10)
    pause(.1)

# finally for all these images, record the motors that didn't move
# approximate positions from this measurement
# (normally you'll want to have this in metadata and pull...)
motor_bsphi = -12.002264999999994  # md['motor_bsphi']
motor_bsx = -16.200218  # md['motor_bsx']
motor_bsy = -14.899795  # md['motor_bsy']
detector_SAXS_distance_m = md['detector_SAXS_distance_m']


img = simg.image/simg_mask.image
# create the mask from the file (keeping the shape)
msk = MaskCreator(data=img)
# when done:

print("Type resume() when done")


def resume():
    global filename
    mask = msk.mask
    # now prepare data
    # this isn't 0,0 anymore since it comes from stitched image
    # have mask from above
    refpoint = simg.refpoint
    refpoint_lab = detector_refy, detector_refx
    scl = .172, .172  # for pilatus300

    import os.path
    # im explicit here but its the mask dir + detector_key + some name
    mask_path = "~/tmp"
    mask_path = os.path.expanduser(mask_path)
    filename = mask_path + "/mask_pilatus300_master_saxs.npz"

    kwargs = dict()
    # kwargs.update(startdoc)
    kwargs['mask'] = mask
    kwargs['refpoint'] = refpoint
    kwargs['refpoint_lab'] = refpoint_lab
    kwargs['scl'] = scl
    # the motor positions used to define the mask
    kwargs['motor_bsphi'] = motor_bsphi
    kwargs['motor_bsx'] = motor_bsx
    kwargs['motor_bsy'] = motor_bsy
    kwargs['detector_SAXS_distance_m'] = detector_SAXS_distance_m
    # saving here (uncomment)
    np.savez(filename, **kwargs)

    figure(0)
    clf()
    imshow(simg.image/simg_mask.image)
