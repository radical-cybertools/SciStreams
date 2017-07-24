# test the XSAnalysis Streams, make sure they're working properly
from SciStreams.analyses.XSAnalysis.Streams import circavg
from SciStreams.analyses.XSAnalysis.tools import xystitch_accumulate

import numpy as np


def test_circavg():
    # def circavg(image, q_map=None, r_map=None,  bins=None, mask=None,
    # **kwargs):
    x = np.linspace(-5, 5, 10)
    X, Y = np.meshgrid(x, x)

    r_map = np.sqrt(X**2 + Y**2)
    # some scaling, randomly chosen
    q_map = r_map**1.1
    image = np.random.random((10, 10))

    # just make sure they don't return errors
    mask = np.ones_like(image)
    res = circavg(image, q_map=q_map, r_map=r_map, bins=None, mask=mask)
    mask = None
    res = circavg(image, q_map=q_map, r_map=r_map, bins=None, mask=mask)

    # if these are not true the data format has changed
    # this is likely okay, but need to reflect in test...
    assert hasattr(res, 'args')
    assert hasattr(res, 'kwargs')

    assert 'sqx' in res.kwargs
    assert 'sqxerr' in res.kwargs
    assert 'sqy' in res.kwargs
    assert 'sqyerr' in res.kwargs


def test_xystitch_accumulate():
    # mostly make sure it runs with no errors
    img = np.zeros((100, 100), dtype=int)
    mask = np.ones((100, 100), dtype=float)
    origin = (40., 50)
    stitchback = True

    prevstate = img, mask, origin, stitchback
    newstate = img, mask, origin, stitchback
    res = xystitch_accumulate(prevstate, newstate)
    assert res[0].shape == img.shape
    assert res[1].shape == mask.shape
