from .detectors2D import detectors2D
import numpy as np
from PIL import Image

from SciStreams.config import master_masks, mask_config
from SciStreams.data.Mask import MaskGenerator, MasterMask


# TODO : need to fix again...
def generate_mask(**md):
    if 'override' in md:
        # override mask generator with simple loader
        # override should be filename
        # TODO : make read different formats
        mask = np.load(md['override'])

        # load the blemish
        detector_key = md['detector_key']
        blemish_kwargs = mask_config[detector_key]['blemish']
        blemish = np.array(Image.open(blemish_kwargs['filename']))
        if blemish.ndim == 3:
            blemish = blemish[:, :, 0]

        return dict(mask=mask*blemish)

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
    '''
    detector_key = 'pilatus300_image'

    pilatus_masks = master_masks[detector_key]
    # the keys that contain data to describe the mask
    # TODO : make neater once thought is more complete
    # have to add .item() because I saved as npy file
    # don't have time to resolve this now

    # required keys and tolerances
    # keys = [doc[0] for doc in pilatus_masks['motors']]
    # tolerances = [doc[1] for doc in pilatus_masks['motors']]
    keys = ['motor_bsphi', 'motor_bsx', 'motor_bsy']
    # the errors tolerated in the positions
    tolerances = [.1, .3, .3]

    master_mask = find_best_mask(pilatus_masks, keys, tolerances, md)

    # now the blemish
    blemish_kwargs = mask_config[detector_key]['blemish']
    blemish = np.array(Image.open(blemish_kwargs['filename']))
    if blemish.ndim == 3:
        blemish = blemish[:, :, 0]

    if master_mask is not None:
        mmg = MaskGenerator(master_mask, blemish=blemish)

        print("Generating mask")
        # hard coded
        motory = md['motor_SAXSy']
        motorx = md['motor_SAXSx']
        mask = mmg.generate(motory, motorx)
    else:
        # get detector shape from det file and make empty mask
        detector_name = _make_detector_name_from_key(detector_key)
        shape = detectors2D[detector_name]['shape']['value']
        msg = "No suitable mask found, "
        msg += "generating ones with shape {}".format(shape)
        print(msg)
        motor_list = {key: md[key] for key in keys}
        print("Motors : {}".format(motor_list))

        mask = np.ones(shape)

    return mask


def generate_mask_pilatus2M(**md):
    ''' generate mask from startdocument information

        Mask generation is not very simple.
        Across detector, it's easy since it's just a shift
        However, when other components move, this is not simple.
        For example, the beamstop shadow depends on the distance of beamtop to
        sample, detector to sample, and the beam x, y position.

        I noticed a 10 pixel discrepancy when moving the beamstop from what
        would have been expected when shifiting it.

        Therefore, another approach may be easier. We save masks for each
        relevant motor position that is not SAXSx and SAXSy for now.
        If we find we would benefit from interpolation, then we can write more
        complex code to solve these problems. For now, I don't think they're
        necessary. They solve such a small use case that they are not worth the
        time considering all the other problems we have left to solve.


        The way this mask generator works is as follows:
            1. Search database for a mask that contains motor positions close
            to the ones given.
            2. When found, shift this mask appropriately so that it aligns with
            the current detector position.
    '''
    detector_key = 'pilatus2M_image'

    pilatus_masks = master_masks[detector_key]
    # the keys that contain data to describe the mask
    # TODO : make neater once thought is more complete
    # have to add .item() because I saved as npy file
    # don't have time to resolve this now

    # required keys and tolerances
    # keys = [doc[0] for doc in pilatus_masks['motors']]
    # tolerances = [doc[1] for doc in pilatus_masks['motors']]
    keys = ['motor_bsphi', 'motor_bsx', 'motor_bsy']
    # the errors tolerated in the positions
    tolerances = [10, 1, 1]

    master_mask = find_best_mask(pilatus_masks, keys, tolerances, md)

    # now the blemish
    blemish_kwargs = mask_config[detector_key]['blemish']
    blemish = np.array(Image.open(blemish_kwargs['filename']))
    if blemish.ndim == 3:
        blemish = blemish[:, :, 0]

    if master_mask is not None:
        mmg = MaskGenerator(master_mask, blemish=blemish)

        print("Generating mask")
        # hard coded
        motory = md['motor_SAXSy']
        motorx = md['motor_SAXSx']
        mask = mmg.generate(motory, motorx)
    else:
        # get detector shape from det file and make empty mask
        detector_name = _make_detector_name_from_key(detector_key)
        shape = detectors2D[detector_name]['shape']['value']
        msg = "No suitable mask found, "
        msg += "generating ones with shape {}".format(shape)
        print(msg)
        motor_list = {key: md[key] for key in keys}
        print("Motors : {}".format(motor_list))
        print("Available masks : {}".format(pilatus_masks))
        mask = np.ones(shape)

    return mask


def _make_detector_name_from_key(name):
    # remove last "_" character
    return name[::-1].split("_", maxsplit=1)[-1][::-1]


def find_best_mask(masks, keys, tolerances, md):
    ''' Search file system for best mask that matches
        the keys keys with errors smaller than tolerances (tolerance).

        masks:  a parameter tree of parameters for each mask
           it includes its filename. This allows more convenient lookup of
           masks (i.e. you don't have to keep reading each mask file to
           retrieve metadata)
           This is also written in hopes to have this saved the databroker way
            (i.e. filename will be a descriptor that gets sent to an asset
            handler)

        md : the metadata

        each mask is also an npz array with more information than just the mask
        use MasterMask to conveniently load it.
    '''
    # search for the right mask that matches data
    retrieved_mask = None
    # find the first mask that matches all motors
    found_mask = False
    for mask_params in masks:
        if found_mask:
            break
        filename = mask_params['filename']
        found_mask = True
        for key, tolerance in zip(keys, tolerances):
            print("comparing {} with {}".format(mask_params[key], md[key]))
            # err1 = np.abs(mask_params[key]-md[key])
            # if err1 > tolerance:
            a = mask_params[key]
            b = md[key]
            if not np.allclose(a, b, atol=tolerance):
                print("Error too big : {} is not close to {}".format(a, b))
                print("Tolerance : +/- {}".format(tolerance))
                found_mask = False
    if found_mask:
        retrieved_mask = MasterMask(filename)
    else:
        retrieved_mask = None
    return retrieved_mask


# hard coded mask generators built into the library
mask_generators = dict(pilatus300=generate_mask_pilatus300,
                       pilatus2M=generate_mask_pilatus2M)
