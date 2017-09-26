import numpy as np

def findLowHigh(img, maxcts=None, percentage=.05):
    ''' Find the reasonable low and high values of an image
            based on its histogram.
            Ignore the zeros

        percentage : percentage of counts to ignore
    '''
    if maxcts is None:
        maxcts = 65536
    w = np.where((~np.isnan(img.ravel()))*(~np.isinf(img.ravel())))
    minval, maxval = np.min(img.ravel()[w]), np.max(img.ravel()[w])
    # number of bins, make approx 100th of no pixels
    nobins = img.shape[0]*img.shape[1]/100
    bins = np.linspace(minval, maxval, nobins)
    hh, bb = np.histogram(img.ravel()[w], bins=bins, range=(1, maxcts))
    hhs = np.cumsum(hh)
    hhsum = np.sum(hh)
    if hhsum > 0:
        hhs = hhs/np.sum(hh)
        wlow = np.where(hhs > percentage)[0]  # 5%
        whigh = np.where(hhs < (1-percentage))[0]  # 95%
    else:
        # some arbitrary values
        wlow = np.array([1])
        whigh = np.array([10])

    bb_cen = (bb[1:] + bb[:-1])*.5

    if len(wlow):
        low = bb_cen[wlow[0]]
    else:
        low = 0

    if len(whigh):
        high = bb_cen[whigh[-1]]
    else:
        high = maxcts

    if high <= low:
        high = low + 1

    # debugging
    # print("low: {}, high : {}".format(low, high))

    return low, high
