from .detectors2D import detectors2D
import numpy as np
from functools import partial
from PIL import Image

from SciStreams.config import mask_config
from SciStreams.data.Mask import BeamstopXYPhi, MaskFrame, MaskGenerator


# TODO : need to fix again...
def generate_mask(**md):
    # get name from det key
    detector_key = md.get('detector_key', None)
    detector_name = detector_key[::-1].split("_",1)[-1][::-1]
    md['detector_name'] = detector_name

    mask_gen = mask_generators[detector_name]

    mask = mask_gen(**md)

    return dict(mask=mask)


def generate_mask_pilatus300(motor_bsphi, motor_bsx, motor_bsy, beamx0, beamy0,
        **kwargs):
    ''' Generate a mask from detector_key and metadata md

        Parameters
        ----------
        detector_key : the key for the image for the detector
        **md : the metadata
            should contain `motor_bsphi`, `motor_bsx`, `motor_bsy`
            `detector_SAXS_x0_pix` and `detector_SAXS_y0_pix`
            which are the degrees of freedom for the beamstop and beam center
    '''
    detector_key = "pilatus300" + "_image"
    # TODO : allow for more than one detector
    #detector_key = md['detectors'][0] + "_image"
    # TODO : Speed up by moving this outside of function
    detector_mask_config = mask_config[detector_key]

    bstop_kwargs = detector_mask_config['beamstop']
    frame_kwargs = detector_mask_config['frame']
    blemish_kwargs = detector_mask_config['blemish']

    beamstop_detector = partial(BeamstopXYPhi, **bstop_kwargs)
    frame_detector = MaskFrame(**frame_kwargs)

    # the known degrees of freedom
    bsphi = motor_bsphi
    bsx = motor_bsx
    bsy = motor_bsy

    obstruction = beamstop_detector(bsphi=bsphi, bsx=bsx,bsy=bsy) + frame_detector
    blemish = np.array(Image.open(blemish_kwargs['filename']))
    if blemish.ndim == 3:
        blemish = blemish[:,:,0]

    mmg = MaskGenerator(obstruction=obstruction, blemish=blemish)
    x0 = beamx0['value']
    y0 = beamy0['value']
    origin = y0, x0

    print("Generating mask")
    mask = mmg.generate(origin)

    return mask


def generate_mask_pilatus2M(**md):
    detector_key = md.get('detector_key', 'pilatus2M')
    # generate a mask from the metadata
    detector_name = detector_key[::-1].split("_",1)[-1][::-1]
    detector_shape = detectors2D[detector_name]['shape']['value']
    print("detector shape : {}".format(detector_shape))
    mask = np.ones(detector_shape)
    return dict(mask=mask)

# hard coded mask generators built into the library
mask_generators = dict(pilatus300=generate_mask_pilatus300,
                       pilatus2M=generate_mask_pilatus2M)
