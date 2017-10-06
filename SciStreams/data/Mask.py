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


class MasterMask:
    def __init__(self, filename):
        ''' Create a master mask.
            Loads from numpy file

            Expects file to have these:
                mask : the mask
                refpoint : the y, x (row, col) coordinates of the reference
                point
                refpoint_lab : the y, x lab coordinates of the reference
                scl : the y, x scale factors when converting from pixel to lab
                frame
        '''
        self.filename = filename
        res = np.load(filename)
        self.mask = res['mask']
        self.refpoint = res['refpoint']
        # y, x coordinates
        self.refpoint_lab = res['refpoint_lab']
        # scale factors for lab/ref conversion
        # TODO : replace with a more general CoordinateTransform Object
        # which has "to_lab" and "from_lab" methods if needed
        self.scl = res['scl']


# TODO add decorators for sciresults
# for now, can't make into a sciresult
# @run_default("XSAnalysis_MaskGenerator", False, False, False, True)
class MaskGenerator:
    ''' A  master mask.
        This is motor position specific.

        A master mask has same pixel size as detector pixel size
        but it can contain more pixels than a detector.
        So it must be described by an x, y position in lab coordinates for the
        dtector and a px, py position for the first pixel in the detector in
        this mask
    '''
    def __init__(self, master_mask, blemish, usermask=None, **kwargs):
        ''' Generate mask from known master mask.

            Take in a Master Mask object with the detector blemish and optional
            usermask.

            Parameters
            ----------

            master_mask : a master_mask object

            blemish : np.ndarray or Mask object
                a Mask object specifyin the detector mask

            user : np.ndarray or Mask object, optional
                a Mask object specifying the user mask

            Note
            ----

            This is meant for SAXS detectors that do not tilt. For SAXS
            detectors with tilting, or WAXS detectors, a different method may
            be necessary.

        '''
        self.mastermask = master_mask.mask

        # calib point on detector, only useful when this quantity changes
        # position of (0,0) coordinate of detector in this mask
        self.refpoint = master_mask.refpoint
        # position of detector in lab when mask is made
        self.refpoint_lab = master_mask.refpoint_lab

        self.scl = master_mask.scl
        # self.masterrefpoint = obstruction.refpoint
        self.blemish = blemish
        self.usermask = usermask

    def generate(self, pos_lab, **kwargs):
        # pos_lab must be in y, x coordinates
        # position : the current detector positions (should be same as
        # refpoint_lab units)
        # find out how much detector has moved by
        posy_lab, posx_lab = pos_lab
        shifty_lab = (posy_lab - self.refpoint_lab[0])
        shiftx_lab = (posx_lab - self.refpoint_lab[1])
        # the shift in pixel space is negative and scaled by mm/pix scale
        # factor
        shifty_pix = -shifty_lab/self.scl[0]
        shiftx_pix = -shiftx_lab/self.scl[1]
        # the 0,0 coordinate moves by this amount
        refpoint = self.refpoint[1]+shifty_pix, self.refpoint[0]+shiftx_pix

        # give master mask, the reference point, blemish file
        mask = make_submask(self.mastermask, refpoint,
                            shape=self.blemish.shape,
                            blemish=self.blemish)
        return mask


def make_submask(master_mask, refpoint, shape, blemish=None):
    ''' Make a submask from the master mask,
        knowing the master_cen center and the outgoing image
        shape and center subimg_cen.

        refpoint is in row,col format (y,x)
            This is the reference point that should register
                masks together
        # using interpolation for subpixel shifts
    '''
    x_master = np.arange(master_mask.shape[1]) - refpoint[1]
    y_master = np.arange(master_mask.shape[0]) - refpoint[0]

    interpolator = RegularGridInterpolator((y_master, x_master), master_mask,
                                           bounds_error=False, fill_value=1)

    # make submask
    x = np.arange(shape[1])
    y = np.arange(shape[0])
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
        # rotation_center not needed here
        self.mmask_frame, self.origin_frame, rotation_center =\
            load_master_mask(filename)
        super(MaskFrame, self).__init__(self.mmask_frame, self.origin_frame)


class BeamstopXYPhi(Obstruction):
    ''' Definition of the beamstop at the CMS beamline with an XY and Phi
    degree of freedom
        This is meant to register a beamstop with a detector.

        All given motor positions are in lab coordinates:
            bsx, bsy : the x and y directions of beamstop shift
            bsx_ref, bsy_ref : the x and y reference points of beamstop
            detx, detx : detector positions
            detx_ref, detx_ref : detector ref positions

        The ref position is the position that the beamstop and detector have to
        be in lab frame to obtain the image with the given reference point.

    '''
    # TODO : Add z projection as well?
    # just a transformation
    # hard coded reference positions
    # TODO : take into account detector shifts
    ''' hard coded values for the beamstop.'''
    def __init__(self,
                 bsphi_lab,
                 bsx_lab, bsy_lab, detx_lab, dety_lab,
                 bsphi_ref_lab,
                 bsx_ref_lab, bsy_ref_lab,
                 detx_ref_lab, dety_ref_lab,
                 filename,
                 sclx=1, scly=1):
        ''' Create a beamstop from a mask, origin pair
            origin is y0, x0

            bsphi : phi rotation
            bsx : x transaltion of beamstop (in pixels)
            bsy : y transaltion of beamstop (in pixels)
        '''
        # first load master mask
        mask, refpoint, rotation_center = load_master_mask(filename)
        print("making beamstop mask")
        print("orig refpoint : {}".format(refpoint))
        print("orig rot center : {}".format(rotation_center))

        # 1. Given image (from filename) where ref motor positions recorded
        # are (need to list *all* motors for any components involved,
        # for now, just beamstop and detector), in *lab frame*:
        # bsx_ref_lab, bsy_ref_lab
        # SAXSx_ref_lab, SAXSy_ref_lab
        dx_bstop_lab = bsx_lab - bsx_ref_lab
        dy_bstop_lab = bsy_lab - bsy_ref_lab
        dx_detx_lab = detx_lab - detx_ref_lab
        dy_dety_lab = dety_lab - dety_ref_lab

        # 2. compute the shift of each reference
        bstop_shiftx_lab = dx_bstop_lab - dx_detx_lab
        bstop_shifty_lab = dy_bstop_lab - dy_dety_lab

        # TODO : Maybe use general coordinated object if not complex
        # 3. now convert to detector space
        bstop_shiftx_detp = bstop_shiftx_lab/sclx
        bstop_shifty_detp = bstop_shifty_lab/scly

        print("shift refpoint by {}, {}".format(bstop_shiftx_detp,
              bstop_shifty_detp))
        # 4. the ref point now needs to be updated according to this shift
        refpointy, refpointx = refpoint
        refpointx = (refpointx + bstop_shiftx_detp)
        refpointy = (refpointy + bstop_shifty_detp)
        refpoint = refpointy, refpointx
        print("new refpoint : {}".format(refpoint))

        # now figure out the rotation
        dphi_bstop_lab = bsphi_lab - bsphi_ref_lab

        super(BeamstopXYPhi, self).__init__((mask == 0).astype(np.float),
                                            refpoint=refpoint,
                                            rotation_center=rotation_center)
        # now rotate
        print("rotate by {}".format(-dphi_bstop_lab))
        self.rotate(-dphi_bstop_lab)

    def _generate_beamstop_mask(self, bsphi, bsx, bsy):
        # change origin
        # obs.origin = 786, 669
        rotangle = -(bsphi - self.ref_bsphi)
        shftx = -(bsx - self.ref_bsx)
        shfty = -(bsy - self.ref_bsy)

        print("rotating by {} degrees".format(rotangle))
        print("shifting by {} in x,y".format(shftx, shfty))

        self.rotate(rotangle)
        self.shiftx(shftx)
        self.shifty(shfty)
        # self.shiftx(-(bsx - self.ref_bsx)/self.dx)
        # self.shifty(-(bsy - self.ref_bsy)/self.dy)
