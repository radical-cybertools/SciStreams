import numpy as np
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from ..core.StreamDoc import Arguments
from .partitioning import center2edge

def circavg(image, q_map=None, r_map=None,  bins=None, mask=None, **kwargs):
    ''' computes the circular average.'''
    # TODO : could avoid creating a mask to save time
    if mask is None:
        mask = np.ones_like(image)

    # figure out bins if necessary
    if bins is None:
        # guess q pixel bins from r_map
        if r_map is not None:
            # choose 1 pixel bins (roughly, not true at very high angles)
            # print("rmap not none, mask shape : {}, rmap shape :
            # {}".format(mask.shape, r_map.shape))
            pxlst = np.where(mask == 1)
            nobins = int(np.max(r_map[pxlst]) - np.min(r_map[pxlst]) + 1)
            # print("rmap is not none, decided on {} bins".format(nobins))
        else:
            # crude guess, I'll be off by a factor between 1-sqrt(2) or so
            # (we'll have that factor less bins than we should)
            # arbitrary number
            nobins = int(np.maximum(*(image.shape))//4)

        # here we assume the rbins uniform
        bins = nobins
        # rbinstat = RadialBinnedStatistic(image.shape, bins=nobins,
        # rpix=r_map, statistic='mean', mask=mask)
        rbinstat = BinnedStatistic1D(r_map.reshape(-1), statistic='mean',
                                     bins=nobins, mask=mask.ravel())
        bin_centers = rbinstat(q_map.ravel())
        bins = center2edge(bin_centers)

    # now we use the real rbins, taking into account Ewald curvature
    # rbinstat = RadialBinnedStatistic(image.shape, bins=bins, rpix=q_map,
    # statistic='mean', mask=mask)
    print("qmap shape : {}".format(q_map.shape))
    print("number bins : {}".format(bins))
    print("mask shape : {}".format(mask.shape))
    rbinstat = BinnedStatistic1D(q_map.reshape(-1), statistic='mean',
                                 bins=bins, mask=mask.ravel())
    sqy = rbinstat(image.ravel())
    sqx = rbinstat.bin_centers
    # get the error from the shot noise only
    # NOTE : variance along ring could also be interesting but you
    # need to know the correlation length of the peaks in the rings... (if
    # there are peaks)
    rbinstat.statistic = "sum"
    noperbin = rbinstat(mask.ravel())
    sqyerr = np.sqrt(rbinstat(image.ravel()))
    sqyerr /= np.sqrt(noperbin)
    # the error is just the bin widths/2 here
    sqxerr = np.diff(rbinstat.bin_edges)/2.

    return Arguments(sqx=sqx, sqy=sqy, sqyerr=sqyerr, sqxerr=sqxerr)
