import numpy as np
from scipy.ndimage.interpolation import rotate as scipy_rotate
from ..processing.stitching import xystitch_accumulate
from .StitchedImage import StitchedImage


class Obstruction(StitchedImage):
    ''' General obstruction on a detector. This is used to generate a mask.
    Origin is the refpoint of the absolute coordinate system that all
    obstructions should align to.

    NOTE : An obstruction is defined 1 where it obstructs and 0 otherwise. This
    is opposite of mask.
    NOTE #2 : adding and subtracting these can results in larger arrays holding
    the obstruction
    NOTE #3 : this assumes binary images (and uses _thresh for threshold)

    NOTE #4 : the mask property of this object cannot be edited

    image : image of the obstruction : 1 is present, 0 absent
    refpoint : the refpoint of the obstruction

    _thresh : a tweak to separate 0's from 1's in the case of floats (i.e.
        rotation/interpolation)
    '''
    _thresh = .5

    def __init__(self, image, refpoint, rotation_center=None):
        super(Obstruction, self).__init__(image, refpoint,
                                          rotation_center=rotation_center,
                                          parentclass=Obstruction)

    # the mask is the inverse of the obstruction
    @property
    def mask(self):
        # try threshold other than 1, use .5
        return (self.image < .5).astype(int)
