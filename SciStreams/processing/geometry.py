import numpy as np
# import pint
# geometry stuff

def find_rotation_center(p1, alpha1, p2, alpha2):
    ''' Find the rotation center of the beam stop (in coordinates of p1 and p2)
    for two points p1 and p2.
    p1 and p2 assumed (x,y) pairs
    alpha1, and alpha2 assumed degrees

        Constraints:
            (these assumptions make the calculation easier)
            - It is assumed that alpha goes increasing *counter-clockwise*
                Two points is not enough to know direction of clock
            - It is assumed that p2 is slightly more counter clockwise than p1
                (this is checked for in code, assuming direction is
                counter-clockwise)

    '''
    # Notation: variables assumed vectors unless superceded by 'norm'
    # with the exception of alpha, which denotes an angle always
    # the _u suffix means normalized (unit vector)
    # the _norm suffix means the norm of the vector
    delta_alpha = alpha2 - alpha1
    if delta_alpha < 0 or delta_alpha > 90:
        errormsg = "Error, increment from alpha1 to alpha 1 "
        errormsg += "must be within 0 to 90 degrees"
        raise ValueError(errormsg)

    # make sure they're numpy arrays for terser notation
    p1 = np.array(p1)
    p2 = np.array(p2)

    # convert to radians as well, we don't need degrees anymore
    # we can take abs value since we assume direction
    delta_alpha = np.radians(np.abs(delta_alpha))

    # some more vectors
    # the midpoint
    p_m = (p1+p2)/2.
    # the midpoint norm
    p_m_norm = np.hypot(*p_m)

    # the vector from p2 to p1
    # note it points towards p1, this is just my convention
    # but this now needs to be assumed for rest
    delta_p = p1-p2
    # the norm of delta_p
    delta_p_norm = np.hypot(*delta_p)

    # the height of the isoceles triangle from the rotation
    # is related to half the midpoint norm (draw out isoceles triangle)
    # this height is not to be confused with the radius of curvature, 
    # which we don't need to solve this as well
    h_norm = (.5*p_m_norm)/np.tan(delta_alpha/2.)

    # the direction of h is cross product of (0, 0, 1) with (p_m[0], p_m[1], 0)
    # for a counter clockwise alpha and p2 at further alpha than p1, 
    # within 90 degree rotation
    # instead of doing cross product, i just explicitly write result from cross
    # prod (deltap_y, -deltap_x)/|delta_p|
    h_u = np.array([delta_p[1], -delta_p[0]])/delta_p_norm

    # the vector pointing to the origin
    h = h_u*h_norm

    # the origin is the midpoint plus the h vector
    po = h + p_m

    return po

# TODO : detx, dety etc should be class eventually
# (but leave as is for now until settled)
def pixel_to_lab(x, y, detx, dety, detx_ref, dety_ref, dpx=1):
    ''' detector pixel coordinates to lab coordinates
        x, y : units of pixels
        detx, dety : detector position in pixels
        detx_ref, dety_ref : detector x and y reference positions
        dpx : conversion of pixels to meters


        assumes square pixels
    '''
    # TODO : maybe use pint library for units
    # (currently a bug where I can't re-define pixel)
    return x*dpx + detx - detx_ref, y*dpx + dety - dety_ref
