

class CoordinateTransform:
    '''
        A coordinate transform object.
        It knows how to go back and forth from its coordinate frame to the lab
        frame. Lab frame is just general term referring to a coordinate system
        that other coordinate systems can transform to.
    '''
    def __init__(self, to_lab, from_lab, lab=None):
        '''
            Parameters
            ----------
            to_lab : function to transform from internal coordinate system to
            the lab frame

            from_lab : function to transform from lab frame to internal
            coordinate system

            lab : CoordinateTransform instance, optional
                if not None, assumes this node transforms to the lab frame
                (root frame)
        '''
        self._to_lab = to_lab
        self._from_lab = from_lab

    def from_lab(self, *coord, **kwargs):
        return self._from_lab(*coord, **kwargs)

    def to_lab(self, *coord, **kwargs):
        return self._to_lab(*coord, **kwargs)

# testing quickly

def to_lab(x, y):
    return 2*x, 2*y

def from_lab(x, y):
    return x/2., y/2.

cc = CoordinateTransform(to_lab, from_lab)


