from SciStreams.data.Obstructions import Obstruction
import numpy as np
from numpy.testing import assert_array_almost_equal

def test_obstruction():
    mask = np.ones((10,10))
    mask[4:5, 6:9] = 0
    origin = 4, 5
    obs = Obstruction(mask, origin)
    # this should not change...
    assert_array_almost_equal(obs.origin, [4, 5])

    assert np.all(obs.mask[4:5, 6:9]==0)

    obs2 = Obstruction(mask, origin)
    obs2.rotate(180, rotation_offset=(10,10))
    # the coordinates should shift this way, tested
    assert_array_almost_equal(obs2.origin, [-16, -17])

    # if this yields an error, try plotting combinations
    # of obs, obs.origin, obs.origin-offset
    # obs2, obs2.origin, obs2.origin-offset
    # in a larger array
