class Obstruction:
    ''' General obstruction on a detector. This is used to generate a mask.
    Origin is the origin of the absolute coordinate system that all
    obstructions should align to.

    NOTE : An obstruction is defined 1 where it obstructs and 0 otherwise. This
    is opposite of mask.
    NOTE #2 : adding and subtracting these can results in larger arrays holding
    the obstruction
    NOTE #3 : this assumes binary images (and uses _thresh for threshold)

    NOTE #4 : the mask property of this object cannot be edited

    image : image of the obstruction : 1 is present, 0 absent
    origin : the origin of the obstruction

    _thresh : a tweak to separate 0's from 1's in the case of floats (i.e.
        rotation/interpolation)

    basis : pass basis from a previous obstruction


    '''
    _thresh = .5
    def __init__(self, mask, origin):
        # invert image
        self.image = (mask.astype(int) < 1).astype(int)
        self.origin = origin

    @property
    def mask(self):
        return (self.image < 1).astype(int)

    def __add__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this.

            Patched a bit so I can reuse stitching method.
        '''
        prevstate = self.image.copy(), np.ones_like(self.image), self.origin, True
        nextstate = newob.image, np.ones_like(newob.image), newob.origin, True
        newstate = xystitch_accumulate(prevstate, nextstate)

        image, mask, origin, stitch = newstate

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = Obstruction((image < self._thresh).astype(int), origin)

        return retobj

    def __sub__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this'''
        prevstate = self.image.copy(), np.ones_like(self.image), self.origin, True
        nextstate = -1*newob.image, np.ones_like(newob.image), newob.origin, True
        newstate = xystitch_accumulate(prevstate, nextstate)
        img, mask, origin, stitch = newstate

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = Obstruction((img < self._thresh).astype(int), origin)

        return retobj

    def shiftx(self, dx):
        self.origin = self.origin[0], self.origin[1] - dx

    def shifty(self, dy):
        self.origin = self.origin[0] - dy, self.origin[1]

    def rotate(self, phi, rotation_offset=None):
        ''' rotate the obstruction in phi, in degrees.

            rotation_offset : offset of origin from center of rotation
                this amounts to a different origin being returned. Resultant
                images are the same

            Note : scipy's new center is center of the full image rotated
                this means that the old origin will be rotated by the vector pointing
                from the center to the origin, floating point number

        '''
        if rotation_offset is None:
            rotation_offset = 0, 0

        rotation_offset = np.array(rotation_offset)
        mask = self.mask
        cen = (np.array(mask.shape)-1)//2
        old_shape = np.array(mask.shape)

        # get original origin
        origin = self.origin
        # compute the rotation center
        rotation_center = origin + rotation_offset

        # rotate image, the easy part. Expands image to keep all pixels
        rotimg = scipy_rotate(mask, phi, reshape=True)

        # now we need to figure out where the new origin is
        phir = np.radians(phi)
        rotation  = np.array([
            [np.cos(phir), np.sin(phir)],
            [-np.sin(phir), np.cos(phir)],
        ])
        # origin-cen vector in original image
        # attn: coordinate is [y,x]
        dr = np.array(rotation_center) - cen

        # rotation is around center, and image is expanded
        # so we need to figure out where it is now
        # rotate the vector of CEN to rotation_center
        new_rotation_center = np.tensordot(dr, rotation, axes=(0,0))
        # now add the center
        new_rotation_center = new_rotation_center + cen

        # now we need to figure out how much image expanded/contracted by
        new_shape = np.array(rotimg.shape)
        dN = new_shape-old_shape
        # shift by delta N/2 (since both sides assumed to expand equally)
        # Attn : susceptible to off by one error
        new_rotation_center += dN/2.

        # shift back in place
        new_origin = new_rotation_center - rotation_offset

        self.origin = new_origin
        self.image = (rotimg > self._thresh).astype(int)

    def _center(self, img, origin):
        ''' center an image to array center.'''
        # center an image to origin
        # find largest dimension first
        dimx = int(2*np.max([origin[1], img.shape[1]-origin[1]-1])+1)
        dimy = int(2*np.max([origin[0], img.shape[0]-origin[0]-1])+1)
        # make new array with these dimensions
        newimg = np.zeros((dimy, dimx))

        cen = newimg.shape[0]//2, newimg.shape[1]//2
        y0 = int(cen[0]-origin[0])
        x0 = int(cen[1]-origin[1])
        newimg[y0:y0+img.shape[0], x0:x0+img.shape[1]] = img
        return newimg, cen


