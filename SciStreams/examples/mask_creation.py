# make the mask for cms
# first load the data for cms
# now making the CMS mask
import os

import numpy as np
from matplotlib.pyplot import ion, figure, clf, clim, imshow, pause, plot


from SciStreams.interfaces.databroker.databases import databases
from SciStreams.config import config

from SciStreams.tools.MaskCreator import MaskCreator

# need this to work with stitched images
from SciStreams.data.StitchedImage import StitchedImage
from PIL import Image

from SciStreams.detectors.detectors2D import detectors2D
from SciStreams.detectors.detectors2D import _make_detector_name_from_key

cmsdb = databases['cms:data']
ion()


'''
Instructions:
    These are instructions for the SAXS detector which depends on the motors:
        motor_SAXSx, motor_SAXSy
    For WAXS detector, more work needs to be done. For example,
        coordinatetransforms need to be created.

    # use start_mask to run uids
    1. start_mask(uids, detector_key)
        # this will stitch based on your uids

'''

def start_mask(uids, detector_key, plot_intermediate=True):
    ''' start masking.

        needs the uids and the detector key (to get blemish file and other
        details like pixel size)

        Parameters
        ----------
        uids : str or list of str
            the uids to search for

        detector_key : the detector key to obtain the mask for

        plot_intermediate : bool, optional
            whether or not to plot intermediate results

        This will raise a KeyError if the following are missing:
            sample_exposure_time
            motor_SAXSx
            motor_SAXSy
            detector_SAXS_distance_m
            motor_bsphi
            motor_bsx
            motor_bsy
    '''
    global img, simg, simg_mask, msk

    # CMS stuff
    det_key = detector_key
    #det_key = "pilatus300_image"

    maskdir = config['maskdir'] + "/" + det_key
    detector_name = _make_detector_name_from_key(det_key)

    det_conf = detectors2D[detector_name]

    # search for blemish
    file_list = os.listdir(maskdir)
    blemish_fname = None

    for fname_tmp in file_list:
        if 'blemish' in fname_tmp:
            blemish_fname = maskdir + "/" + fname_tmp
            break

    if blemish_fname is not None:
        # load blemish
        blemish = np.array(Image.open(blemish_fname))
    else:
        # if not, just load ones with shape
        blemish = np.ones(det_conf['shape']['value'])

    mask = blemish


    hdrs = list(cmsdb[uids])

    Nhdrs = len(hdrs)
    # for pilatus300
    # must be y, x
    #pixel_scl = .172, .172
    dpx = det_conf['pixel_size_x']['value']
    dpy = det_conf['pixel_size_y']['value']
    pixel_scl = dpy, dpx
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
            print("detector is initially at {} (y,x)".format((detector_refy,
                                                              detector_refx)))
            refpoint = 0, 0
            simg = StitchedImage(img, refpoint)
            simg_mask = StitchedImage(blemish, refpoint)
        else:
            detectory, detectorx = md['motor_SAXSy'], md['motor_SAXSx']
            print("detector is now at {} (y,x)".format((detectory, detectorx)))
            shifty, shiftx = detectory-detector_refy, detectorx-detector_refx
            # has to be y, x in general
            shifty_pix, shiftx_pix = shifty/pixel_scl[0], shiftx/pixel_scl[1]
            refpoint = shifty_pix, shiftx_pix

            simg = simg + StitchedImage(img, refpoint)
            simg_mask = simg_mask + StitchedImage(mask, refpoint)

        if plot_intermediate:
            figure(2)
            clf()
            imshow(simg.image/simg_mask.image)
            plot(simg.refpoint[1], simg.refpoint[0], 'ro')
            clim(0, 10)
            pause(.1)
    figure(2)
    clf()
    imshow(simg.image/simg_mask.image)
    plot(simg.refpoint[1], simg.refpoint[0], 'ro')
    clim(0, 10)
    pause(.1)

    # finally for all these images, record the motors that didn't move
    # approximate positions from this measurement
    # (normally you'll want to have this in metadata and pull...)
    motor_bsphi = md['motor_bsphi']
    motor_bsx = md['motor_bsx']
    motor_bsy = md['motor_bsy']
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
