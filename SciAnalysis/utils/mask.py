import numpy as np
from scipy.interpolate import RegularGridInterpolator

def make_submask(master_mask, master_cen, shape, subimg_cen):
    ''' Make a submask from the master mask,
        knowing the master_cen center and the outgoing image
        shape and center subimg_cen.
    '''
    x_master = np.arange(master_mask.shape[1]) - master_cen[1]
    y_master = np.arange(master_mask.shape[0]) - master_cen[0]

    interpolator = RegularGridInterpolator((y_master, x_master), master_mask)#, bounds_error=False, fill_value=1)

    # make submask
    x = np.arange(shape[1]) - subimg_cen[1]
    y = np.arange(shape[0]) - subimg_cen[0]
    X, Y = np.meshgrid(x, y)
    points = (Y.ravel(), X.ravel())
    # it's a linear interpolator, so we just cast to ints (non-border regions should just be 1)
    submask = interpolator(points).reshape(shape).astype(int)

    return submask
