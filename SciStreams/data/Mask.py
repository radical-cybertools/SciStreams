import numpy as np
from scipy.interpolate import RegularGridInterpolator
from .CoordinateTransform import CoordinateTransform


class MasterMask:
    ''' This loads a master mask from file
    '''
    # TODO : Perhaps file loading and master mask should be separated?
    # (note : may not be practical)
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
        self.reload(filename)

    def reload(self, filename):
        ''' load a master mask format '''
        res = np.load(filename)
        self.mask = res['mask']
        self.refpoint = res['refpoint']
        # y, x coordinates
        self.refpoint_lab = res['refpoint_lab']
        # scale factors for lab/ref conversion
        # TODO : replace with a more general CoordinateTransform Object
        # which has "to_lab" and "from_lab" methods if needed
        self.scl = res['scl']


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

        # print("scale {}".format(self.scl))
        # TODO : scl should be positive, not checked here
        try:
            len(self.scl)
        except TypeError:
            self.scl = [self.scl]

        if len(self.scl) == 1:
            self.scl = [self.scl[0], self.scl[0]]

        # these are the implicit transforms for this mask
        # NOTE : for detectors that rotate or do anything other than
        # translation this will not be sufficient. However, it the "to_lab"
        # and "from_lab" terminology will likely make this code extendable
        def to_lab(py, px):
            ''' Note : returns y,x
                The scaling is inverted (-1) when going from lab to pix and
                vice versa.
            '''
            lx = (-1)*(px-self.refpoint[1])*self.scl[1] + self.refpoint_lab[1]
            ly = (-1)*(py-self.refpoint[0])*self.scl[0] + self.refpoint_lab[0]
            return ly, lx

        def from_lab(ly, lx):
            ''' Note : returns y,x
                lx, ly : position in lab coordinates
                The scaling is inverted (-1) when going from lab to pix and
                vice versa.
            '''
            # print("moving from {} to {}".format((ly, lx), self.refpoint_lab))
            px = (-1)*(lx-self.refpoint_lab[1])/self.scl[1] + self.refpoint[1]
            py = (-1)*(ly-self.refpoint_lab[0])/self.scl[0] + self.refpoint[0]
            # print("new refpoint : {}".format((px,py)))
            return py, px

        self.trans = CoordinateTransform(to_lab, from_lab)

        # self.masterrefpoint = obstruction.refpoint
        self.blemish = blemish
        self.usermask = usermask

    def generate(self, *pos_lab):
        # print("lab coordinates requested : {}".format(pos_lab))
        # print("original lab coordinates for lab ref point :
        #       {}".format(self.refpoint_lab))
        # get the reference point according to the new positions
        refpoint = self.trans.from_lab(*pos_lab)

        # print("generating mask with refpoint {}".format(refpoint))
        # print("Mask refpoint is {}".format(self.refpoint))
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

        NOTE : Internally, all reference points refer as y, x
    '''
    # print("refpoint is {}".format(refpoint))
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

    import matplotlib.pyplot as plt
    plt.figure(6)
    plt.clf()
    plt.imshow(submask)
    plt.clim(0, 2)

    return submask*(submask > 0.5)
