# same as test_pipeline but distributed
import tempfile
from PIL import Image
import os
import numpy as np
import time
from cache import cache

from nose.tools import assert_true, assert_false
from numpy.testing import assert_array_almost_equal

# Dask stuff
from SciAnalysis.config import delayed
from dask.delayed import Delayed
from distributed import Client
_pipeline_client = Client("10.11.128.3:8786")

from databroker.broker import Header

# Sources and Sinks
import SciAnalysis.interfaces.databroker.databroker as source_databroker
import SciAnalysis.interfaces.file.reading as source_file

# Some extra detilas
from SciAnalysis.interfaces.detectors import detectors2D

# this is the intermediate interface
from SciAnalysis.interfaces.SciResult import SciResult, parse_sciresults

from SciAnalysis.analyses.XSAnalysis.Protocols import LoadSAXSImage, LoadCalibration, CircularAverage

def test_sciresult_parser():
    ''' This test ensures taht 
        The inputs and outputs of functions are properly 
            normalized using SciResult.

        The inputs can be SciResults or explicit arguments
        and the output is a sciresult with only one entry
            with name _output_name
    '''
    @parse_sciresults("My Protocol", attributes={})
    def foo(a=1, **kwargs):
        return dict(a=a)

    test = SciResult(a=1)
    test['attributes']['myattr'] = "my attribute"
    test['attributes']['anotherattr'] = "another attribute"
    test['global_attributes'] = ['myattr']

    res = foo(a=test)
    assert res['attributes']['myattr'] == "my attribute"
    assert 'anotherattr' not in res['attributes']
    assert res.get() == 1


    scires = SciResult(test=1)
    assert 'test' in scires['outputs']
    scires = scires({'test' : 'foo'})
    assert 'foo' in scires['outputs']
    assert 'test' not in scires['outputs']


def test_sciresult():
    ''' Just ensure instance checking is fine for SciResult.'''
    # necessary when trying to distinguish SciResult from dict
    assert_true(isinstance(SciResult(), dict))
    assert_false(isinstance(dict(), SciResult))


def test_calibration():
    # TODO : Replace with a portable db to to the db testing
    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires = source_databroker.pull(dbname="cms:data", uid=uid)
    scires_data = SciResult(calibration=scires['attributes'])
    scires_data = scires_data("calibration")
    calib_protocol = LoadCalibration(calibration=scires_data)
    calibres = calib_protocol()
    assert isinstance(calibres, Delayed)
    calibres = calibres.compute()
    assert isinstance(calibres, SciResult)
    assert calibres("calibration").get()['beamx0']['value'] == 379.0
    #print(calibres)

    calib_protocol = LoadCalibration()
    calib_protocol.set_keymap("None")
    calib_protocol.add(name='beamx0', value=50, unit='pixel')
    calib_protocol.add(name='beamy0', value=50, unit='pixel')
    calib_protocol.add(name='detectors', value=['pilatus300'], unit='N/A')
    calibres = calib_protocol().compute()
    calibdict = calibres('calibration').get()
    assert calibdict['beamx0']['value'] == 50
    #print(calibres)
    
def test_load_saxs_img(plot=False, output=False):
    ''' test the load_saxs_img class'''
    # I randomly chose some header
    # TODO : source should be managed in a function, need to think more about
    # it
    uid = '0b2a6007-ab1f-4f09-99ad-74c1c0a4b391'
    scires_header = source_databroker.pull('cms:data', uid=uid)
    scires_header = scires_header('pilatus300_image')

    # making temporary data
    tmpdir_data = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_data)

    # make dummy data
    img_shape = (100,100)
    data = np.ones(img_shape, dtype=np.uint8)
    data[50:60] = 0
    data_filename = tmpdir_data + "/test_data.png"
    im = Image.fromarray(data)
    im.save(data_filename)

    scires_datafile = source_file.FileDesc(data_filename).get()

    # testing that protocol can take a SciResult or data
    # test with data
    img_load_protocol = LoadSAXSImage()
    res_fileinput = img_load_protocol(infile=scires_datafile)
    # test with sciresult
    #print(scires_header)
    # TODO : try adding attributes
    #sample_name = scires_header['attributes']['sample_name']
    #sample_uid = scires_header['attributes']['uid']

    # A protocol

    res_headerinput = img_load_protocol(infile=scires_header('pilatus300_image'))
    res_headerinput = res_headerinput.compute()


    if plot:
        import matplotlib.pyplot as plt
        plt.ion()
        plt.figure(0);plt.clf()
        plt.imshow(res_headerinput['image'])

    return res_headerinput.get()

def test_circular_average(plot=False, output=False):

    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_header = source_databroker.pull('cms:data', uid = uid)
    scires_dbattr = SciResult(calibration=scires_header['attributes'])
    scires_dbattr = scires_dbattr('calibration')
    scires_header = scires_header('pilatus300_image')

    # make dummy data
    tmpdir_data = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_data)
    img_shape = (100,100)
    data = np.ones(img_shape, dtype=np.uint8)
    data[50:60] = 0
    data_filename = tmpdir_data + "/test_data.png"
    im = Image.fromarray(data)
    im.save(data_filename)


    #calibres = load_calibration(calibration=header).run()
    calibration_protocol = LoadCalibration()
    calibration_protocol.set_keymap("cms")
    scires_calib = calibration_protocol(calibration=scires_dbattr)('calibration').compute()
    # retrieve the latest calibration
    latest_calib = source_databroker.pull('cms:analysis',  protocol_name="XS:calibration")
    #print(latest_calib)

    image_load_protocol = LoadSAXSImage()
    image = image_load_protocol(infile=scires_header)

    circ_avg_protocol = CircularAverage()
    scires_circavg = circ_avg_protocol(image = image, calibration=scires_calib)
    scires_circavg = scires_circavg.compute()

    latest_calib = source_databroker.pull('cms:analysis', protocol_name="XS:calibration")
    latest_circavg = source_databroker.pull('cms:analysis', protocol_name="XS:circular_average")

    if output:
        return latest_circavg
