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
    # get name from det key
    detector_key = md.get('detector_key', None)
    detector_name = detector_key[::-1].split("_", 1)[-1][::-1]
    md['detector_name'] = detector_name

    # ensure detector_name exists, else give no mask
    if detector_name in mask_generators:
        print("calling function {}".format(mask_generators[detector_name]))
        mask_gen = mask_generators[detector_name]

        mask = mask_gen(**md)
    else:
        mask = None

    return dict(mask=mask)


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
            beam_center_x0
            beam_center_y0
        in hdf5 file.
    '''
    f = h5py.File(fname, "r")
    # make a copy before closing
    if 'beam_center_x0' not in f:
        print("Did not find 'beam_center_x0'")
        return None
    if 'beam_center_y0' not in f:
        print("Did not find 'beam_center_y0'")
        return None
    beamx0 = np.array(f['beam_center_x0'])
    beamy0 = np.array(f['beam_center_y0'])
    f.close()
    return dict(beamx0=beamx0, beamy0=beamy0)

def generate_mask_pilatus2M(**md):
    ''' generate mask from startdocument information

        This will read from local config files for the mask
        filenames if they exist.
    '''
    detector_key = 'pilatus2M_image'

    mask_config = masks_config.get(detector_key, {})

    # get filenames from config
    blemish_fname = mask_config.get('blemish', {}).get('filename', None)
    mask_fname = mask_config.get('mask', {}).get('filename', None)
    # get the shape from config
    mask_shape = mask_config['shape']

    if blemish_fname is not None:
        blemish = load_blemish(blemish_fname)
    else:
        blemish = None

    if mask_fname is not None:
        mask = load_mask(mask_fname)
        beam_center = load_beamcenter(mask_fname)
    else:
        print("No mask found, ignoring")
        mask = None
        beam_center = None

    # now interpolate the beam center from the actual beam center
    beamcenterx = md.get('detector_saxs_x0_pix', None)
    beamcentery = md.get('detector_saxs_y0_pix', None)
    if beamcenterx is not None and beamcentery is not None:
        beam_center_experiment = dict(beam_center_x0=beamcenterx,
                                      beam_center_y0=beamcentery,
                                      )
    else:
        beam_center_experiment = None
        print("No beam center found")

    mask_expt = None
    if mask is not None and beam_center_experiment is not None:
        mask_expt = make_subimage(mask, beam_center, beam_center_experiment)
    else:
        if mask is None:
            print("No mask found")
        if beam_center_experiment is None:
            print("No beam center found")

    if blemish is not None:
        if mask_expt is None:
            mask_expt = blemish
        else:
            mask_expt = blemish*mask_expt

    # just in case, make it a float
    mask_expt = mask_expt.astype(float)

    return mask_expt


# hard coded mask generators built into the library
mask_generators = dict(pilatus300=generate_mask_pilatus300,
                       pilatus2M=generate_mask_pilatus2M)


# helper functions
from scipy.interpolate import RegularGridInterpolator
import numpy as np

def make_subimage(master_image, refpoint, shape, new_refpoint):
    ''' Make a subimage from the master image.

        Parameters
        ----------
        master_image: 2d np.ndarray
            The master image

        refpoint: number
            The reference point

        shape: 2-tuple
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
    # print("refpoint is {}".format(refpoint))
    x_master = np.arange(master_image.shape[1]) - refpoint[1]
    y_master = np.arange(master_image.shape[0]) - refpoint[0]

    interpolator = RegularGridInterpolator((y_master, x_master), master_image,
                                           bounds_error=False, fill_value=0)

    # make sub image, also make origin the ref point
    x = np.arange(shape[1]) - new_refpoint[1]
    y = np.arange(shape[0]) - new_refpoint[0]
    X, Y = np.meshgrid(x, y)
    points = (Y.ravel(), X.ravel())
    # it's a linear interpolator, so we just cast to ints (non-border regions
    # should just be 1)
    subimage = interpolator(points).reshape(shape)
    return subimage
