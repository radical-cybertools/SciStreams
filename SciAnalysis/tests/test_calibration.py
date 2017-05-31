# test the calibration objects
from nose.tools import assert_almost_equal
from numpy.testing import assert_array_almost_equal

import numpy as np

def test_calib():
    ''' Test the original calibration object.'''
    from SciAnalysis.analyses.XSAnalysis.Data import Calibration

    calib = Calibration(wavelength_A=1.0, distance_m=5.0, pixel_size_um=172,
                     width=300, height=400, x0=100, y0=200)

    assert_almost_equal(calib.q_per_pixel, .000216, places=3)
    q_map = calib.q_map()
    assert_array_almost_equal(q_map.ravel()[::32000], np.array([0.04832965,
                                                                0.02966395,
                                                                0.00280984,
                                                                0.03376203]),
                              decimal=3)

    # this was used for plotting
    #from pylab import *
    #ion()

    #figure(0);clf()
    #imshow(q_map)

def test_calib_rqconv():
    ''' Test the new calibration object against the original.
        Doesn't test thoroughly for all elements.
        If an error flags here, maybe something with the conversion is wrong somewhere
    '''
    from SciAnalysis.analyses.XSAnalysis.Data import Calibration
    from SciAnalysis.analyses.XSAnalysis.DataRQconv import CalibrationRQconv

    calib_old = Calibration(wavelength_A=1.0, distance_m=5.0, pixel_size_um=172,
                     width=300, height=400, x0=100, y0=200)
    calib = CalibrationRQconv(wavelength_A=1.0, distance_m=5.0, pixel_size_um=172,
                     width=300, height=400, x0=100, y0=200)

    assert_almost_equal(calib.q_per_pixel, .000216, places=3)
    q_map = calib.q_map()
    q_map_old = calib_old.q_map()
    assert_array_almost_equal(q_map, q_map_old)
