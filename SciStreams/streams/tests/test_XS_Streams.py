# test the XSAnalysis Streams, make sure they're working properly
from SciStreams.core.StreamDoc import StreamDoc
from SciStreams.streams.XS_Streams import ImageStitchingStream,\
        CalibrationStream, CircularAverageStream, QPHIMapStream,\
        ThumbStream


from SciStreams.detectors.detectors2D import detectors2D

import numpy as np

from numpy.testing import assert_array_almost_equal


def test_CalibrationStream_pilatus():
    ''' Test the calibration stream
        for cms data.
        # TODO : Should generalize this
            so it's beam line independent
        # TODO : test other qmaps (not just qmap)
    '''
    keymap_name = 'cms'
    detector = 'pilatus300'

    sin, sout = CalibrationStream(keymap_name=keymap_name, detector=detector)
    L = list()
    sout.map(L.append)

    data = dict(
        calibration_wavelength_A=1.0,
        detector_SAXS_x0_pix=5.0,
        detector_SAXS_y0_pix=5.0,
        detector_SAXS_distance_m=5.0,
    )
    sdoc = StreamDoc(args=data)
    sin.emit(sdoc)

    # now get the calibration object
    calib = L[0]['args'][0]
    qmap = calib.q_map
    # assert detector shape is correct
    # should be (619, 487) but here it's detector independent
    assert qmap.shape == detectors2D[detector]['shape']['value']

    # this is pilatus specific
    assert_array_almost_equal(qmap[200:210, 300], np.array([0.07642863,
                                                           0.07654801,
                                                           0.07666781,
                                                           0.07678804,
                                                           0.07690868,
                                                           0.07702974,
                                                           0.07715122,
                                                           0.07727311,
                                                           0.07739541,
                                                           0.07751812]))


def test_CircularAverageStream():
    ''' Test the circular average stream'''
    sin, sout = CircularAverageStream()

    L = list()
    sout.map(L.append)

    mask = None
    bins = 3
    img = np.random.random((10, 10))
    x = np.linspace(-5, 5, 10)
    X, Y = np.meshgrid(x, x)
    r_map = np.sqrt(X**2 + Y**2)
    q_map = r_map*.12

    class Calib:
        def __init__(self, qmap, rmap):
            self.q_map = qmap
            self.r_map = rmap

    calibration = Calib(q_map, r_map)

    sdoc = StreamDoc(args=[img, calibration],
                     kwargs=dict(mask=mask, bins=bins))

    sin.emit(sdoc)

    return L


def test_QPHIMapStream():
    ''' Test the qphimap stream'''
    bins = (3, 4)
    sin, sout = QPHIMapStream(bins=bins)

    L = list()
    sout.map(L.append)

    mask = None
    img = np.random.random((10, 10))
    x = np.linspace(-5, 5, 10)
    X, Y = np.meshgrid(x, x)
    # r_map = np.sqrt(X**2 + Y**2)
    # q_map = r_map*.12

    origin = (3, 3)

    sdoc = StreamDoc(args=[img],
                     kwargs=dict(origin=origin, mask=mask))

    sin.emit(sdoc)

    assert(L[0]['kwargs']['sqphi'].shape == bins)


def test_ImageStitchingStream():
    ''' test the image stitching.'''
    sin, sout = ImageStitchingStream()

    L = list()
    sout.map(L.append)

    mask = np.ones((10, 10), dtype=np.int64)
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
                                  stitchback=False))
    sin.emit(sdoc1)

    sdoc2 = StreamDoc(kwargs=dict(mask=mask, image=img2, origin=origin2,
                                  stitchback=True))
    sin.emit(sdoc2)

    sdoc3 = StreamDoc(kwargs=dict(mask=mask, image=img3, origin=origin3,
                                  stitchback=True))
    sin.emit(sdoc3)

    sdoc4 = StreamDoc(kwargs=dict(mask=mask, image=img4, origin=origin4,
                                  stitchback=True))
    sin.emit(sdoc4)

    sdoc5 = StreamDoc(kwargs=dict(mask=mask, image=img5, origin=origin5,
                                  stitchback=False))
    sin.emit(sdoc5)

    # this scenario should only yield one image
    assert len(L) == 1

    # now get the stitch and make sure it was okay
    stitched = L[0]['kwargs']['image']

    assert_array_almost_equal(stitched[:, 2], np.array([0., 0., 0., 0., 0., 1.,
                                                        1., 2., 2., 1., 1., 1.,
                                                        1., 1., 1.]))


def test_ThumbStream():
    sin, sout = ThumbStream()

    L = list()
    sout.map(L.append)

    image = np.ones((100, 100))
    sin.emit(StreamDoc(args=[image]))

    assert isinstance(L[0]['kwargs']['thumb'], np.ndarray)

# rcParams['image.interpolation'] = None
