import numpy as np
from SciStreams.processing.numerical import roundbydigits
from numpy.testing import assert_array_equal


def test_roundbydigits():
    '''test the round by digits function.'''
    res = roundbydigits(123.421421, digits=6)
    assert res == 123.421

    res = roundbydigits(-123.421421, digits=6)
    assert res == -123.421

    res = roundbydigits(123.421421, digits=3)
    assert res == 123.0

    res = roundbydigits(0, digits=3)
    assert res == 0

    res = roundbydigits(np.nan, digits=3)
    assert np.isnan(res)

    res = roundbydigits(np.inf, digits=3)
    assert np.isinf(res)

    res = roundbydigits(np.array([123.421421, 1.1351,
                                  np.nan, np.inf, 0]), digits=6)
    assert_array_equal(res, np.array([123.421, 1.1351, np.nan, np.inf, 0]))
