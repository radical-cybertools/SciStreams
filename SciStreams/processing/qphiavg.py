# import numpy as np
from skbeam.core.accumulators.binned_statistic import RPhiBinnedStatistic
#from ..core.StreamDoc import Arguments


def qphiavg(image, mask=None, bins=None, origin=None):
    ''' quick qphi average calculator.
        ignores bins for now
    '''
    # TODO : replace with method that takes qphi maps
    # TODO : also return q and phi of this...
    # print("In qphi average stream")
    rphibinstat = RPhiBinnedStatistic(image.shape, mask=mask, origin=origin,
            bins=bins)
    sqphi = rphibinstat(image)
    qs = rphibinstat.bin_centers[0]
    phis = rphibinstat.bin_centers[1]
    return dict(sqphi=sqphi, qs=qs, phis=phis)
