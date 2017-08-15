import numpy as np
from SciStreams.processing.stitching import xystitch_accumulate


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
