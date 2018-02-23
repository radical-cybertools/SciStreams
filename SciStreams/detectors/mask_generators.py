from .detectors2D import detectors2D
import numpy as np
from PIL import Image

from SciStreams.config import masks_config

# TODO : need to fix again...
def generate_mask(**md):
    ''' Generate a mask.

        This dispatches the generation to a mask generator function
            depending on the metadata.
    '''
    #print("Calling mask generator")
    # get name from det key
    detector_key = md.get('detector_key', None)
    if detector_key is not None:
        detector_name = detector_key[::-1].split("_", 1)[-1][::-1]
    else:
        detector_name = None
        print("Could not find detector name! (Missing detector_key?)")
        print("metadata : {}".format(md))
    md['detector_name'] = detector_name

    # ensure detector_name exists, else give no mask
    if detector_name in mask_generators:
        print("calling function {}".format(mask_generators[detector_name]))
        mask_gen = mask_generators[detector_name]

        mask = mask_gen(**md)
    else:
        mask = None

    if mask is not None:
        print("A mask has been found and generated.")
    return dict(mask=mask)

import h5py
def load_blemish(fname):
    ''' load blemish file. Just the 'blemish' keyword
        of the file. It is not "mask" to allow for the saving of a
        blemish file with a mask in the same file.'''
    f = h5py.File(fname, "r")
    # make a copy before closing
    blemish = np.array(f['blemish'])
    f.close()
    return blemish

def load_mask(fname):
    ''' load mask
        just reads from hdf5 file.
        Returns a mask and reference point (beam center)
    '''
    f = h5py.File(fname, "r")
    # make a copy before closing
    mask = np.array(f['mask'])
    f.close()
    return mask

def load_beamcenter(fname):
    '''
        load beam center. just looks for
            beam_center_x
            beam_center_y
        in hdf5 file.
    '''
    f = h5py.File(fname, "r")
    # make a copy before closing
    if 'beam_center_x' not in f:
        print("Did not find 'beam_center_x'")
        return None
    if 'beam_center_y' not in f:
        print("Did not find 'beam_center_y'")
        return None
    beamx0 = float(f['beam_center_x'].value)
    beamy0 = float(f['beam_center_y'].value)
    f.close()
    return [beamy0, beamx0]

def generate_mask_pilatus2M(**md):
    ''' generate mask from startdocument information

        This will read from local config files for the mask
        filenames if they exist.
    '''
    #print("loading a mask. md: {}".format(md))
    detector_key = 'pilatus2M_image'

    mask_config = masks_config.get(detector_key, {})

    # get filenames from config
    master_dir = mask_config.get('mask_dir', ".")
    blemish_fname = mask_config.get('blemish', {}).get('filename', None)
    blemish_fname = master_dir + "/" + blemish_fname
    mask_fname = mask_config.get('mask', {}).get('filename', None)
    mask_fname = master_dir + "/" + mask_fname
    # get the shape from config
    #print(mask_config)
    mask_shape = np.array(mask_config['shape'])

    #print('mask shape : {}'.format(mask_shape))

    if blemish_fname is not None:
        blemish = load_blemish(blemish_fname)
    else:
        blemish = None

    if mask_fname is not None:
        mask = load_mask(mask_fname)
        beam_center = load_beamcenter(mask_fname)
        #print("Got beam center from mask: {}".format(beam_center))
        #print("filename {}".format(mask_fname))
    else:
        #print("No mask found, ignoring")
        mask = None
        beam_center = None

    # now interpolate the beam center from the actual beam center
    beamcenterx = md.get('beamx0', None)
    beamcentery = md.get('beamy0', None)

    if isinstance(beamcenterx, dict):
        beamcenterx = beamcenterx.get('value', None)

    if isinstance(beamcentery, dict):
        beamcentery = beamcentery.get('value', None)

    if beamcenterx is not None and beamcentery is not None:
        beam_center_experiment = [beamcentery, beamcenterx]
    else:
        beam_center_experiment = None
        print("No beam center found")

    mask_expt = None
    if mask is not None and beam_center_experiment is not None:
        #print("got a mask!")
        mask_expt = make_subimage(mask, beam_center, mask_shape, beam_center_experiment)
        #print("Made sub image")
    else:
        if mask is None:
            print("No mask found")
        if beam_center_experiment is None:
            print("No beam center found")

    if blemish is not None:
        #print("got a blemish!")
        if mask_expt is None:
            mask_expt = blemish
        else:
            mask_expt = blemish*mask_expt

    # just in case, make it a float
    mask_expt = mask_expt.astype(float)

    return mask_expt


def generate_mask_pilatus300(**md):
    ''' Generate a mask from detector_key and metadata md

        Parameters
        ----------
        detector_key : the key for the image for the detector
        **md : the metadata
            should contain `motor_bsphi`, `motor_bsx`, `motor_bsy`
            `detector_SAXS_x0_pix` and `detector_SAXS_y0_pix`
            which are the degrees of freedom for the beamstop and beam center

        Notes
        -----
        The mask generator takes the position pair as y, x as input, and *not*
        x, y
    '''
    raise NotImplementedError("Error, we don't implement this yet")


# hard coded mask generators built into the library
mask_generators = dict(pilatus300=generate_mask_pilatus300,
                       pilatus2M=generate_mask_pilatus2M)


# helper functions
from scipy.interpolate import RegularGridInterpolator
import numpy as np

def make_subimage(master_image, refpoint, new_shape, new_refpoint):
    ''' Make a subimage from the master image.

        Parameters
        ----------
        master_image: 2d np.ndarray
            The master image

        refpoint: number
            The reference point

        new_shape: 2-tuple
            The desired shape of the sub image

        new_refpoint: number
            The reference point for the new image

        Notes
        ------
        refpoint and new_refpoint are in row,col format (y,x)
            This is the reference point that should register
                images together
        Internally, all reference points refer as y, x
        Interpolation is used for subpixel shifts
    '''
    #print("refpoint is {}".format(refpoint))
    #print("new refpoint is {}".format(new_refpoint))
    #print("master image shape : {}".format(master_image.shape))
    #print("new shape : {}".format(new_shape))
    x_master = np.arange(master_image.shape[1]) - refpoint[1]
    y_master = np.arange(master_image.shape[0]) - refpoint[0]

    #print("initializing interpolator")
    interpolator = RegularGridInterpolator((y_master, x_master), master_image,
                                           bounds_error=False, fill_value=0)

    #print("interpolating")
    # make sub image, also make origin the ref point
    #print('new_shape[1] : {}'.format(new_shape[1]))
    #print('new_refpoint[1] : {}'.format(new_refpoint[1]))
    x = np.arange(int(new_shape[1])) - new_refpoint[1]
    #print("x dim: {}".format(x))
    y = np.arange(new_shape[0]) - new_refpoint[0]
    X, Y = np.meshgrid(x, y)
    points = (Y.ravel(), X.ravel())
    #print("points : {}".format(points))
    # it's a linear interpolator, so we just cast to ints (non-border regions
    # should just be 1)
    subimage = interpolator(points).reshape(new_shape)
    #print("done interpolating")
    return subimage
