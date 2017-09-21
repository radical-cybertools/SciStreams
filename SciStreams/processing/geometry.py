import numpy as np
# import pint
# geometry stuff

def find_rotation_center(r1, alpha1, r2, alpha2):
    ''' Find the rotation center of the beam stop (in coordinates of r1 and r2)
    for two points r1 and r2.
    r1 and r2 assumed (x,y) pairs
    alpha1, and alpha2 assumed degrees

        Constraints:
            (these assumptions make the calculation easier)
            - It is assumed that alpha goes increasing  when moving to r2
                Two points is not enough to know direction of clock
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
    r1 = np.array(r1)
    r2 = np.array(r2)

    # convert to radians as well, we don't need degrees anymore
    # we can take abs value since we assume direction
    delta_alpha = np.radians(np.abs(delta_alpha))

    # some more vectors
    # the midpoint
    rm = (r1+r2)/2.
    # the midpoint norm
    rm_mag = np.hypot(*rm)

    # the vector from r2 to r1
    # note it points towards r1, this is just my convention
    # but this now needs to be assumed for rest
    deltar = r1-r2
    # the norm of delta_p
    deltar_mag = np.hypot(*deltar)

    # the height of the isoceles triangle from the rotation
    # is related to half the midpoint norm (draw out isoceles triangle)
    # this height is not to be confused with the radius of curvature, 
    # which we don't need to solve this as well
    h_mag = (.5*deltar_mag)/np.tan(delta_alpha/2.)

    # the direction of h is cross product of (0, 0, 1) with (r_m[0], r_m[1], 0)
    # for a counter clockwise alpha and r2 at further alpha than r1, 
    # within 90 degree rotation
    # instead of doing cross product, i just explicitly write result from cross
    # prod (deltap_y, -deltap_x)/|delta_p|
    h_hat = np.array([deltar[1], -deltar[0]])/deltar_mag

    # the vector pointing to the origin
    h = h_hat*h_mag

    # the origin is the midpoint plus the h vector
    ro = h + rm

    return ro

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

def lab_to_pixel(labx, laby, detx, dety, detx_ref, dety_ref, dpx=1):
    return (labx-(detx-detx_ref))/dpx, (laby - (dey-dety_ref))/dpx
