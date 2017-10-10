import numpy as np
from scipy.ndimage.interpolation import rotate as scipy_rotate
from ..processing.stitching import xystitch_accumulate


def rotate_custom(image, phi, reshape=True, **kwargs):
    # need to fill in holes from scipy
    return scipy_rotate(image, phi, reshape=reshape, **kwargs)


# TODO : Make this work for subpixel resolution images
# (Not needed yet)
# Stitched image copied from Obstruction object
class StitchedImage:
    ''' General stitched image
        Define each image according to the same refpoint and just add them
            together with the "+" operator.

    NOTE #2 : adding and subtracting results in larger arrays

    image : the resultant image
    refpoint : the refpoint of the image
    rotation_center : the rotation_center, in absolute pixel coordinates
    '''
    def __init__(self, image, refpoint, rotation_center=None,
                 parentclass=None):
        # invert image
        self.image = image.astype(np.float32)
        self.refpoint = np.array(refpoint)
        if parentclass is None:
            parentclass = StitchedImage

        if rotation_center is None:
            rotation_center = 0., 0.
        self.rotation_center = rotation_center
        self.parentclass = parentclass

    def transpose(self):
        ''' transpose the stitched image'''
        self.rotation_center = self.rotation_center[1], self.rotation_center[0]
        self.refpoint = self.refpoint[1], self.refpoint[0]
        self.image = self.image.T

    def __add__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this.

            Patched a bit so I can reuse stitching method.
        '''
        prevstate = self.image.copy(), np.ones_like(self.image),\
            self.refpoint, True
        nextstate = newob.image, np.ones_like(newob.image),\
            newob.refpoint, True
        newstate = xystitch_accumulate(prevstate, nextstate)

        image, mask, refpoint, stitch = newstate
        # update the rotation center
        drefpoint = refpoint - self.refpoint
        rotation_center = self.rotation_center + drefpoint
        rotation_center2 = newob.rotation_center + (refpoint - newob.refpoint)
        print("new obj rotation centers: {} and {}".format(rotation_center,
              rotation_center2))

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = self.parentclass(image, rotation_center=rotation_center,
                                  refpoint=refpoint)

        return retobj

    def __sub__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this'''
        prevstate = self.image.copy(), np.ones_like(self.image),\
            self.refpoint, True
        nextstate = -1*newob.image, np.ones_like(newob.image),\
            newob.refpoint, True
        newstate = xystitch_accumulate(prevstate, nextstate)
        image, mask, refpoint, stitch = newstate

        # update the rotation center
        drefpoint = refpoint - self.refpoint
        rotation_center = self.rotation_center + drefpoint

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = self.parentclass(image, rotation_center=rotation_center,
                                  refpoint=refpoint)

        return retobj

    def shiftx(self, dx):
        self.refpoint = self.refpoint[0], self.refpoint[1] - dx

    def shifty(self, dy):
        self.refpoint = self.refpoint[0] - dy, self.refpoint[1]

    def rotate(self, phi, rotation_offset=None):
        ''' rotate the obstruction in phi in place, in degrees.

            rotation_offset : offset of refpoint from center of rotation
                this amounts to a different refpoint being returned. Resultant
                images are the same
                Normally, you won't need this, and would be better off setting
                the rotation center.

            Note : scipy's new center is center of the full image rotated
                this means that the old refpoint will be rotated by the vector
                pointing from the center to the refpoint, floating point number

        '''
        if rotation_offset is None:
            rotation_offsetx = self.rotation_center[1]-self.refpoint[1]
            rotation_offsety = self.rotation_center[0]-self.refpoint[0]
            rotation_offset = rotation_offsety, rotation_offsetx

        rotation_offset = np.array(rotation_offset)
        image = self.image
        cen = (np.array(image.shape)-1)//2
        old_shape = np.array(image.shape)

        # get refpointal refpoint
        refpoint = self.refpoint
        # compute the rotation center
        rotation_center = refpoint + rotation_offset

        # rotate image, the easy part. Expands image to keep all pixels
        rotimg = rotate_custom(image, phi, reshape=True, mode="constant",
                               cval=1)

        # now we need to figure out where the new refpoint is
        phir = np.radians(phi)
        rotation = np.array([
                             [np.cos(phir), np.sin(phir)],
                             [-np.sin(phir), np.cos(phir)],
                             ])
        # refpoint-cen vector in refpointal image
        # attn: coordinate is [y,x]
        dr = np.array(rotation_center) - cen

        # rotation is around center, and image is expanded
        # so we need to figure out where it is now
        # rotate the vector of CEN to rotation_center
        new_rotation_center = np.tensordot(dr, rotation, axes=(0, 0))
        # now add the center
        new_rotation_center = new_rotation_center + cen

        # now we need to figure out how much image expanded/contracted by
        new_shape = np.array(rotimg.shape)
        dN = new_shape-old_shape
        # shift by delta N/2 (since both sides assumed to expand equally)
        # Attn : susceptible to off by one error
        new_rotation_center += dN/2.

        # shift back in place
        new_refpoint = new_rotation_center - rotation_offset

        # OLD API (don't update the object anymore, create new)
        # self.rotation_center = new_rotation_center
        # self.refpoint = new_refpoint
        # self.image = rotimg

        self.rotation_center = new_rotation_center
        self.refpoint = new_refpoint
        self.image = rotimg

    def _center(self, img, refpoint):
        ''' center an image to array center.'''
        # center an image to refpoint
        # find largest dimension first
        dimx = int(2*np.max([refpoint[1], img.shape[1]-refpoint[1]-1])+1)
        dimy = int(2*np.max([refpoint[0], img.shape[0]-refpoint[0]-1])+1)
        # make new array with these dimensions
        newimg = np.zeros((dimy, dimx))

        cen = newimg.shape[0]//2, newimg.shape[1]//2
        y0 = int(cen[0]-refpoint[0])
        x0 = int(cen[1]-refpoint[1])
        newimg[y0:y0+img.shape[0], x0:x0+img.shape[1]] = img
        return newimg, cen
