import numpy as np
from scipy.interpolate import RegularGridInterpolator
from .Obstructions import Obstruction
from ..utils.mask import load_master_mask


class Mask:
    def __init__(self, mask=None, shape=None):
        ''' just saves a mask in object based on shape.
            Must be an array.
        '''
        if mask is None and shape is None:
            raise ValueError("Error, mask and shape both none")
        if mask is not None:
            self.mask = mask
        else:
            self.mask = np.ones(shape)


# @run_default("XSAnalysis_MasterMask", False, False, False, True)
# Normally, this would inherit mask but we can't because it's not working with
# delayed so I re-write instantiation
class MasterMask:
    def __init__(self, master=None, origin=None, shape=None):
        ''' MasterMask object.
            This is meant to store a larger mask of the experimental setup,
            from where the mask for a detector is generated from.

            Parameters
            ----------
            master : 2d np.ndarray,
                the master mask

            origin : 2d np.ndarray,
                the origin of the master mask
                origin is in row,col format (x,y)

            shape : 2D np.ndarray, optional
                the shape of the mask
        '''
        mask = master
        if mask is None and shape is None:
            raise ValueError("Error, mask and shape both none")
        if mask is not None:
            self.mask = mask
        else:
            self.mask = np.ones(shape)
        self.origin = origin


# TODO add decorators for sciresults
# for now, can't make into a sciresult
# @run_default("XSAnalysis_MaskGenerator", False, False, False, True)
class MaskGenerator:
    ''' A  master mask.'''
    def __init__(self, obstruction, blemish, usermask=None, **kwargs):
        ''' Generate mask from known master mask.

            Take in a Master Mask object with the detector blemish and optional
            usermask.

            Parameters
            ----------

            master : a MasterMask object
                can be a dict with 'mask' and 'origin' members or
                a MasterMask object

            blemish : np.ndarray or Mask object
                a Mask object specifyin the detector mask

            user : np.ndarray or Mask object, optional
                a Mask object specifying the user mask

            Note
            ----

            This is meant for SAXS detectors that do not tilt. For SAXS
            detectors with tilting, or WAXS detectors, a different method may
            be necessary.

            Examples
            --------

            mm = MasterMask(master, blemish, origin)
            # y0 is rows, x0 is columns
            mask = mm.generate((y0,x0))
        '''
        self.mastermask = obstruction.mask
        self.masterorigin = obstruction.origin
        self.load_blemish(blemish)
        self.load_usermask(usermask)

    def rotate_obstruction(self, phi):
        ''' Rotate obstruction in degrees.'''
        # TODO : Add rotate about origin
        pass

    def load_obstruction(self, obstruction):
        self.mastermask = obstruction.mask
        self.masterorigin = obstruction.origin

    def load_blemish(self, blemish):
        try:
            self.blemish = blemish.mask
        except AttributeError:
            self.blemish = blemish

    def load_usermask(self, usermask):
        try:
            self.usermask = usermask.mask
        except AttributeError:
            self.usermask = usermask

    def generate(self, origin=None, **kwargs):
        if origin is None:
            raise ValueError("Need to specify an origin")
        # Note this returns a sciresult object
        mask = make_submask(self.mastermask, self.masterorigin,
                            shape=self.blemish.shape, origin=origin,
                            blemish=self.blemish)
        return mask


def make_submask(master_mask, master_cen, shape=None, origin=None,
                 blemish=None):
    ''' Make a submask from the master mask,
        knowing the master_cen center and the outgoing image
        shape and center subimg_cen.

        origin is in row,col format (x,y)
    '''
    if shape is None or origin is None:
        raise ValueError("Error, shape or origin cannot be None")
    x_master = np.arange(master_mask.shape[1]) - master_cen[1]
    y_master = np.arange(master_mask.shape[0]) - master_cen[0]

    interpolator = RegularGridInterpolator((y_master, x_master), master_mask,
                                           bounds_error=False, fill_value=0)

    # make submask
    x = np.arange(shape[1]) - origin[1]
    y = np.arange(shape[0]) - origin[0]
    X, Y = np.meshgrid(x, y)
    points = (Y.ravel(), X.ravel())
    # it's a linear interpolator, so we just cast to ints (non-border regions
    # should just be 1)
    submask = interpolator(points).reshape(shape).astype(int)
    if blemish is not None:
        submask = submask*blemish

    return submask*(submask > 0.5)


class MaskFrame(Obstruction):
    def __init__(self, filename):
        self.mmask_frame, self.origin_frame = load_master_mask(filename)
        super(MaskFrame, self).__init__(self.mmask_frame, self.origin_frame)


class BeamstopXYPhi(Obstruction):
    ''' Definition of the beamstop at the CMS beamline with an XY and Phi
    degree of freedom'''
    # hard coded reference positions
    ''' hard coded values for the beamstop.'''
    def __init__(self, bsphi, bsx, bsy, ref_bsphi, ref_bsx, ref_bsy, ref_rotx,
                 ref_roty, dx, dy, filename):
        ''' Create a beamstop from a mask, origin pair
            origin is y0, x0

            bsphi : phi rotation
            bsx : x transaltion of beamstop
            bsy : y transaltion of beamstop
        '''
        self.ref_bsphi = ref_bsphi
        self.ref_bsx = ref_bsx
        self.ref_bsy = ref_bsy
        self.rotation_offset = ref_roty, ref_rotx
        self.dx = dx
        self.dy = dy
        self.mmask_bstop, self.origin_bstop = load_master_mask(filename)
        super(BeamstopXYPhi, self).__init__(self.mmask_bstop, self.origin_bstop)
        self._generate_beamstop_mask(bsphi, bsx, bsy)


    def _generate_beamstop_mask(self, bsphi, bsx, bsy):
        # change origin
        #obs.origin = 786, 669
        self.rotate(-(bsphi - self.ref_bsphi), rotation_offset = self.rotation_offset)
        self.shiftx(-(bsx - self.ref_bsx)/self.dx)
        self.shifty(-(bsy - self.ref_bsy)/self.dy)
