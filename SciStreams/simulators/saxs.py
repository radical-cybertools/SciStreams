import numpy as np


def mkSAXS(shape, peaks, peakamps, phase, x1,
           y1, sigma, sym):
    '''
        roughly simulate some saxs pattern. Here it just contains peaks

    '''
    x = np.arange(shape[1]) - x1
    y = np.arange(shape[0]) - y1
    X, Y = np.meshgrid(x, y)
    R = np.hypot(X, Y)
    PHI = np.arctan2(Y, X)

    data = 1./np.sqrt(X**2 + Y**2)
    wbad = np.where(np.isinf(data)+np.isnan(data))
    wgood = np.where(~np.isinf(data)*~np.isnan(data))
    data[wbad] = np.min(data[wgood])
    for peak, amp in zip(peaks, peakamps):
        newpeak = amp*np.exp(-(R-peak)**2/2./sigma**2)
        newpeak = newpeak*(np.cos(sym*(PHI-phase))**2)
        # give peak some symmetry
        data += newpeak

    return data
