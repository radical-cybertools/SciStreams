# test the XSAnalysis Streams, make sure they're working properly
from SciAnalysis.interfaces.StreamDoc import StreamDoc
from SciAnalysis.analyses.XSAnalysis.Streams import ImageStitchingStream,\
    CalibrationStream

import numpy as np

from numpy.testing import assert_array_almost_equal

def test_CalibrationStream():
    ''' Test the calibration stream.'''

def test_ImageStitch():
    ''' test the image stitching.'''
    sin, sout = ImageStitchingStream()

    L = list()
    sout.map(L.append, raw=True)

    mask = np.ones((10, 10))
    img1 = np.ones_like(mask, dtype=float)
    # 3 rows are higher
    img1[2:4] = 2
    # some arb value
    origin1 = [2, 3]

    # roll along zero axis
    img2 = np.roll(img1, 2, axis=0)
    # rolled by two
    origin2 = [2+2, 3]

    # roll along zero axis
    img3 = np.roll(img1, 4, axis=0)
    # rolled by four
    origin3 = [2+4, 3]

    # roll along zero axis
    img4 = np.roll(img1, 4, axis=1)
    # rolled by four in x
    origin4 = [2, 3+4]

    # this one doesn't matter, just will trigger the output of the stitched
    # image
    img5 = np.ones_like(img1)
    origin5 = [2, 10]

    # first image, stitchback can be anything
    sdoc1 = StreamDoc(kwargs=dict(mask=mask, image=img1, origin=origin1,
                                  stitchback=0))
    sin.emit(sdoc1)

    sdoc2 = StreamDoc(kwargs=dict(mask=mask, image=img2, origin=origin2,
                                  stitchback=1))
    sin.emit(sdoc2)

    sdoc3 = StreamDoc(kwargs=dict(mask=mask, image=img3, origin=origin3,
                                  stitchback=1))
    sin.emit(sdoc3)

    sdoc4 = StreamDoc(kwargs=dict(mask=mask, image=img4, origin=origin4,
                                  stitchback=1))
    sin.emit(sdoc4)

    sdoc5 = StreamDoc(kwargs=dict(mask=mask, image=img5, origin=origin5,
                                  stitchback=0))
    sin.emit(sdoc5)

    # this scenario should only yield one image
    assert len(L) == 1

    # now get the stitch and make sure it was okay
    stitched = L[0]['kwargs']['image']

    assert_array_almost_equal(stitched[:, 2], np.array([0., 0., 0., 0., 1., 1.,
                                                       2., 2., 1., 1., 1., 1.,
                                                       1., 1.]))

# rcParams['image.interpolation'] = None
