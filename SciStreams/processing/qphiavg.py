# import numpy as np
from skbeam.core.accumulators.binned_statistic import BinnedStatistic2D
#from ..core.StreamDoc import Arguments



def qphiavg(image, q_map=None, phi_map=None, mask=None, bins=(800, 360), origin=None, range=None,
            statistic='mean'):
    ''' quick qphi average calculator.
        ignores bins for now
    '''
    # TODO : replace with method that takes qphi maps
    # TODO : also return q and phi of this...
    # print("In qphi average stream")
    shape = image.shape
    if origin is None:
        origin = (shape[0] - 1) / 2., (shape[1] - 1) / 2.

    from skbeam.core.utils import radial_grid, angle_grid

    if q_map is None:
        q_map = radial_grid(origin, shape)
    if phi_map is None:
        phi_map = angle_grid(origin, shape)

    expected_shape = tuple(shape)
    if mask is not None:
        if mask.shape != expected_shape:
            raise ValueError('"mask" has incorrect shape. '
                             ' Expected: ' + str(expected_shape) +
                             ' Received: ' + str(mask.shape))
        mask = mask.reshape(-1)

    rphibinstat = BinnedStatistic2D(q_map.reshape(-1), phi_map.reshape(-1),
                                    statistic=statistic, bins=bins, mask=mask,
                                    range=range)

    sqphi = rphibinstat(image.ravel())
    qs = rphibinstat.bin_centers[0]
    phis = rphibinstat.bin_centers[1]
    return dict(sqphi=sqphi, qs=qs, phis=phis)
