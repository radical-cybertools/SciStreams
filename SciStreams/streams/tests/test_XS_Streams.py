# test the XSAnalysis Streams, make sure they're working properly
from SciStreams.core.StreamDoc import StreamDoc
from SciStreams.streams.XS_Streams import ImageStitchingStream,\
        CalibrationStream, CircularAverageStream, QPHIMapStream,\
        ThumbStream, PrimaryFilteringStream, AttributeNormalizingStream


from SciStreams.detectors.detectors2D import detectors2D
from SciStreams.detectors.detectors2D import _make_detector_name_from_key

import numpy as np

from numpy.testing import assert_array_almost_equal

'''
    If anything goes wrong in stream, these tests are a good way to review if
you're using them in the intended manner. Note that this does not mean this
intended manner is the best one. Contributions welcome.



'''


def test_PrimaryFilteringStream():
    # test the primary filtering stream
    detector_name = 'pilatus300'
    detector_name2 = 'pilatus2M'
    detector_key = detector_name + "_image"
    detector_key2 = detector_name2 + "_image"
    img_shape = detectors2D[detector_name]['shape']['value']
    img2_shape = detectors2D[detector_name2]['shape']['value']

    sin, sout = PrimaryFilteringStream()
    L = sout.sink_to_list()

    img = np.random.random(img_shape)
    img2 = np.random.random(img2_shape)
    data = {detector_key: img,
            detector_key2: img2}
    attr = dict()
    sdoc = StreamDoc(attributes=attr, kwargs=data)

    sin.emit(sdoc)
    assert len(L) == 2

    # they may not necessarily come in order, so find keys
    detkey1 = L[0]['attributes']['detector_key']
    detname1 = _make_detector_name_from_key(detkey1)
    img_shape_1 = detectors2D[detname1]['shape']['value']

    detkey2 = L[1]['attributes']['detector_key']
    detname2 = _make_detector_name_from_key(detkey2)
    img_shape_2 = detectors2D[detname2]['shape']['value']

    assert_array_almost_equal(L[0]['kwargs']['image'].shape,
                              img_shape_1)
    assert_array_almost_equal(L[1]['kwargs']['image'].shape,
                              img_shape_2)


def test_AttributeNormalizingStream():
    ''' test the attribute normalizing stream, mainly for CMS data here.'''
    external_keymap = {
                       'beamx0': {
                                  'default_unit': 'pixel',
                                  'default_value': None,
                                  'name': 'detector_SAXS_x0_pix'
                                  },
                       'beamy0': {
                                  'default_unit': 'pixel',
                                  'default_value': None,
                                  'name': 'detector_SAXS_y0_pix'
                                  },
                       'pixel_size_x': {
                                        'default_unit': 'pixel',
                                        'default_value': 'um',
                                        'name': None},
                       'pixel_size_y': {'default_unit': 'pixel',
                                        'default_value': 'um',
                                        'name': None},
                       'sample_det_distance': {
                                               'default_unit': 'm',
                                               'default_value': None,
                                               'name':
                                               'detector_SAXS_distance_m'},
                       'wavelength': {
                                      'default_unit': 'Angstrom',
                                      'default_value': 'None',
                                      'name': 'calibration_wavelength_A'}
                       }
    sin, sout = AttributeNormalizingStream(external_keymap=external_keymap)

    L = sout.sink_to_list()

    attr = dict(
        calibration_wavelength_A=1.0,
        detector_SAXS_x0_pix=5.0,
        detector_SAXS_y0_pix=5.0,
        detector_SAXS_distance_m=5.0,
        detector_key='pilatus300_image',
    )
    # array shape doesn't matter here
    data = dict(image=np.ones((10, 10)))

    sdoc = StreamDoc(kwargs=data, attributes=attr)

    # now emit data
    sin.emit(sdoc)

    assert len(L) == 1
    kwargs = L[0]['kwargs']
    assert 'wavelength' in kwargs
    assert 'sample_det_distance' in kwargs
    assert 'beamx0' in kwargs
    assert 'beamy0' in kwargs
    assert 'pixel_size_x' in kwargs
    assert 'pixel_size_y' in kwargs


def test_CalibrationStream():
    ''' Test the calibration stream
        for cms data.
        # TODO : Should generalize this
            so it's beam line independent
        # TODO : test other qmaps (not just qmap)
    '''
    detector_name = 'pilatus300'
    img_shape = detectors2D[detector_name]['shape']['value']

    sin, sout = CalibrationStream()
    L = sout.sink_to_list()

    attr = dict(
        # assumed Angs for now
        wavelength=dict(value=1.0),
        # this has to be um for now
        pixel_size_x=dict(value=172),
        pixel_size_y=dict(value=172),
        shape=img_shape,
        beamx0=dict(value=5.0),
        beamy0=dict(value=5.0),
        # this has to be m for now
        sample_det_distance=dict(value=5.0),
    )
    sdoc = StreamDoc(kwargs=attr)
    sin.emit(sdoc)

    # now get the calibration object
    calib = L[0]['kwargs']['calibration']
    qmap = calib.q_map
    # assert detector shape is correct
    # should be (619, 487) but here it's detector independent
    assert_array_almost_equal(qmap.shape,
                              detectors2D[detector_name]['shape']['value'])

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

    L = sout.sink_to_list()

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

    sdoc = StreamDoc(kwargs=dict(image=img,
                                 calibration=calibration,
                                 mask=mask,
                                 bins=bins))

    # just make sure goes without errors
    sin.emit(sdoc)

    assert len(L) == 1
    data = L[0]['kwargs']
    assert len(data['sqx'] == bins)


def test_QPHIMapStream():
    ''' Test the qphimap stream'''
    bins = (3, 4)
    sin, sout = QPHIMapStream(bins=bins)

    L = sout.sink_to_list()

    mask = None
    img = np.random.random((10, 10))
    x = np.linspace(-5, 5, 10)
    X, Y = np.meshgrid(x, x)
    # r_map = np.sqrt(X**2 + Y**2)
    # q_map = r_map*.12

    origin = (3, 3)

    sdoc = StreamDoc(kwargs=dict(image=img,
                                 origin=origin,
                                 mask=mask))

    sin.emit(sdoc)

    assert_array_almost_equal(L[0]['kwargs']['sqphi'].shape, bins)


def test_ImageStitchingStream():
    ''' test the image stitching.'''
    sin, sout = ImageStitchingStream()

    L = sout.sink_to_list()

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

    L = sout.sink_to_list()

    image = np.ones((100, 100))
    sin.emit(StreamDoc(args=[image]))

    assert isinstance(L[0]['kwargs']['thumb'], np.ndarray)

# rcParams['image.interpolation'] = None
