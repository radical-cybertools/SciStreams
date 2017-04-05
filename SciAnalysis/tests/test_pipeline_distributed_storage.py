# same as test_pipeline but distributed
import tempfile
from PIL import Image
import os
import numpy as np
import time
from cache import cache
from toolz import curry

from nose.tools import assert_true, assert_false
from numpy.testing import assert_array_almost_equal

# Dask stuff
from dask import delayed
from dask.delayed import Delayed
from distributed import Client
_pipeline_client = Client("10.11.128.3:8786")



from databroker.broker import Header

# SciAnalysis Stuff

# I/O stuff
import SciAnalysis.interfaces.databroker.databases as dblib
#from SciAnalysis.interfaces.databroker.dbtools import store_results_databroker, HeaderDict, pull
# each source/sink should be named source_something by convention
import SciAnalysis.interfaces.databroker.dbtools as source_dbtools
import SciAnalysis.interfaces.file.reading as source_file
from SciAnalysis.interfaces.detectors import detectors2D

# this is the intermediate interface
from SciAnalysis.interfaces.SciResult import SciResult, parse_sciresults

from SciAnalysis.protocols.Protocol import Protocol, run_default


# initialize database
databases = dblib.initialize()
cddb = databases['cms']['data']
cadb = databases['cms']['analysis']

# TODO : add run_default
# TODO : add run_explicit
# TODO : how to handle implicit arguments? (Some global maybe?)
# TODO : add databroker keymap
# TODO : Header should have .get() routine which gives function output
# TODO : decorator for file storage

'''
For now, assume all incoming arguments are well defined each step.

There will be a case where this is not true.
For ex : linecut -> need to backpropagate until the latest data set that
is computed is found. It will be necessary to figure out what to fill in
for missing arguments for that data set.

Ideas introduced:
1. SciResult : this is a dictionary of results. It may contain data
    stored in filestore. 
2. new class specifiers : 
    _name : some unique name for protocol
    _depends : dependencies of the arguments of the protocol
        {'_arg0' : ..., '_arg1' : ..., ... 'foo' : ...}
        _argn means nth argument, rest are keywords
    _func_args : the explicit arguments for function
    _keymap : the keymap of results
3. two step run process:
    result = myclass(**kwargs).run(**moreoverridingargs).compute()
'''

#

# interface side stuff
# This is the databroker version
class LoadSAXSImage:
    def __init__(self, **kwargs):
        self.kwargs= kwargs

    def __call__(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(**new_kwargs)

    @delayed(pure=False)
    #@store_results('cms')
    @run_default
    # TODO have it get class name
    @parse_sciresults("XS:LoadSAXSImage")
    def run(infile=None, **kwargs):
        # TODO : Add a databroker interface
        if isinstance(infile, np.ndarray):
            img = infile
        elif isinstance(infile, str):
            img = np.array(Image.open(infile))
        else:
            raise ValueError("Sorry, did not understand the input argument: {}".format(infile))

        return img


class LoadCalibration:
    '''
        Loading a calibration, two step process:

        calib_protocol = load_cms_calibration()
        calib_protocol.add('beamx0', 50, 'pixel')
        calib_protocol.add('beamy0', 50, 'pixel')

        calib = calib_protocol(energy=13.5)

        Notes
        -----
            1. Arguments can be added either with 'add' or during function call
            2. If using dask, need to run calib.compute() to obtain result
    '''
    # defaults of function
    _defaults= {'wavelength' : {'value' : None, 'unit' : 'Angstrom'},
                 'beamx0' : {'value' : None, 'unit' : 'pixel'},
                 'beamy0' : {'value' : None, 'unit' : 'pixel'},
                 'sample_det_distance' : {'value' : None, 'unit' : 'm'},
                # Area detector specific entries:
                 # width is columns, height is rows
                 #'AD_width' : {'value' : None, 'unit' : 'pixel'},
                 #'AD_height' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_x' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_y' : {'value' : None, 'unit' : 'pixel'},
                  #TODO : This assumes data has this detector, not good to use, remove eventually
                 'detectors' : {'value' : ['pilatus300'], 'unit' : None},
        }
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.kwargs['calib_defaults'] = self._defaults
        self.set_keymap("cms")

    def __call__(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(**new_kwargs)

    def add(self, name=None, value=None, unit=None):
        self.kwargs.update({name : {'value' : value, 'unit' : unit}})

    def set_keymap(self, name):
        if name == "cms":
            self.kwargs['calib_keymap'] = {'wavelength' : {'key' : 'calibration_wavelength_A',
                                        'unit' : 'Angstrom'},
                        'detectors' : {'key' : 'detectors',
                                        'unit' : 'N/A'},
                        'beamx0' : {'key' : 'detector_SAXS_x0_pix', 
                                    'unit' : 'pixel'},
                        'beamy0' : {'key' : 'detector_SAXS_y0_pix',
                                    'unit' : 'pixel'},
                        'sample_det_distance' : {'key' : 'detector_SAXS_distance_m',
                                                 'unit' : 'pixel'}
            }
        elif name == "None":
            self.kwargs['calib_keymap'] = {'wavelength' : {'key' : 'wavelength',
                                        'unit' : 'Angstrom'},
                        'detectors' : {'key' : 'detectors',
                                        'unit' : 'N/A'},
                        'beamx0' : {'key' : 'beamx0', 
                                    'unit' : 'pixel'},
                        'beamy0' : {'key' : 'beamy0',
                                    'unit' : 'pixel'},
                        'sample_det_distance' : {'key' : 'sample_det_distance',
                                                 'unit' : 'pixel'}
            }
        else:
            raise ValueError("Error, cannot find keymap for loading calibration")


    # this is an unbound method
    @delayed(pure=True)
    @source_dbtools.store_results('cms')
    @run_default
    @parse_sciresults("XS:calibration")
    def run(calibration={}, **kwargs):
        '''
            Load calibration data from a SciResult's attributes.

            The data must be a dictionary.
            either:
                load_calibration(calibration=myCalib)
            or:
                load_calibration(wavelength=dict(value=13.5, unit='keV')) etc
            
            This is area detector specific.
        '''
        calib_keymap = kwargs['calib_keymap']
        calib_defaults = kwargs['calib_defaults']
    
        # a map from Header start doc to data
        # TODO : move out of function

        calibration = calibration.copy()
        # update calibration with all keyword arguments
        # fill in the defaults
        for key, val in calib_defaults.items():
            if key not in calibration:
                calibration[key] = val
        # now override with kwargs
        for key, val in kwargs.items():
            calibration[key] = val

        calib_tmp = dict()
        # walk through defaults
        for key in calib_defaults.keys():
            if key in calib_keymap:
                entry = calib_keymap[key]
                start_key = entry['key'] # get name of key
            else:
                entry = calib_defaults[key]
                start_key = key # get name of key

            unit = entry['unit']
            val = calibration.get(start_key, calib_defaults[key])
            # it can be a number (like from header) or a dict, check
            if isinstance(val, dict) and 'value' in val:
                val = val['value']
            calib_tmp[key] ={'value' : val, 'unit' : unit}

        # finally, get the width and height by looking at first detector in header
        # TODO : add ability to read more than one detector, maybe in calib_keymap
        if isinstance(calibration[calib_keymap['detectors']['key']], dict):
            first_detector = calibration[calib_keymap['detectors']['key']]['value'][0]
        else:
            first_detector = calibration[calib_keymap['detectors']['key']][0]

        detector_key = detectors2D[first_detector]['image_key']['value']

        # look up in local library
        pixel_size_x = detectors2D[first_detector]['pixel_size_x']['value']
        pixel_size_x_unit = detectors2D[first_detector]['pixel_size_x']['unit']
        pixel_size_y = detectors2D[first_detector]['pixel_size_y']['value']
        pixel_size_y_unit = detectors2D[first_detector]['pixel_size_y']['unit']

        img_shape = detectors2D[first_detector]['shape']

        calib_tmp['pixel_size_x'] = dict(value=pixel_size_x, unit=pixel_size_x_unit)
        calib_tmp['pixel_size_y'] = dict(value=pixel_size_y, unit=pixel_size_y_unit)
        calib_tmp['shape'] = img_shape.copy() #WARNING : copies only first level, this is one level dict
        calibration = calib_tmp
        
    
        return dict(calibration=calibration)
        

class CircularAverage:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(**new_kwargs)

    @delayed(pure=True)
    @source_dbtools.store_results('cms:analysis', {'sqx' : 'npy', 'sqy' : 'npy'})
    @run_default
    @parse_sciresults("XS:CircularAverage")
    def run(image=None, calibration=None, bins=100, mask=None, **kwargs):
        #print(calibration)
        #print("computing")
        # TODO : remove when switched to mongodb
        # This is an sqlite problem
        if isinstance(calibration, str):
            import json
            calibration = json.loads(calibration)
        x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']
        from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic
        img_shape = tuple(calibration['shape']['value'])
        #print(img_shape)
        rbinstat = RadialBinnedStatistic(img_shape, bins=bins, origin=(y0,x0), mask=mask)
        sq = rbinstat(image)
        sqx = rbinstat.bin_centers

        return dict(sqx=sqx, sqy=sq)

class FitPeaks:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(**new_kwargs)

    @delayed(pure=True)
    @source_dbtools.store_results('cms', {'best_fit' : 'npy'})
    @run_default
    @parse_sciresults("XS:FitPeaks")
    def run(sqx=None, sqy=None, init_guess=None, npeaks=None, **kwargs):
        if sqx is None or sqy is None:
            raise ValueError("sqx, sqy or q0 not defined")
        if npeaks is None:
            npeaks = 1

        if init_guess is None:
            init_guess = {}

        import lmfit
        params = lmfit.Parameters()
        for i in range(npeaks):

            key = "A{}".format(i)
            params.add(key, value=1.)
            # update with init_guess if exists
            if key in init_guess:
                params[key].value = init_guess[key]
            key = "x0{}".format(i)
            params.add(key.format(i), value=0.)
            # update with init_guess if exists
            if key in init_guess:
                params[key].value = init_guess[key]
            key = "sigma{}".format(i)
            params.add(key.format(i), value=0.1)
            # update with init_guess if exists
            if key in init_guess:
                params[key].value = init_guess[key]

        def gausspeaks(x, npeaks, params):
            res = np.zeros_like(x)
            for i in range(npeaks):
                amp = params["A{}".format(i)]
                x0 = params["x0{}".format(i)]
                sigma = params["sigma{}".format(i)]
                res += amp*np.exp(-(x-x0)**2/2./sigma**2)
            return res

        def minimizer(params, npeaks, sqx, sqy):
            params = params.valuesdict()
            res = gausspeaks(sqx, npeaks, params)
            return (res-sqy)

        results = lmfit.minimize(minimizer, params, args=(npeaks, sqx, sqy), method='leastsq')
        fit_params = results.values
        fit_params['npeaks'] = npeaks
        fit_stats = dict(chisqr=results.chisqr)
        best_fit = gausspeaks(sqx, npeaks, fit_params)

        return dict(fit_params=fit_params, fit_stats=fit_stats,
                    best_fit=best_fit)




# Completed tests (above are WIP)
#
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


# works in dask but not distributed
def test_delayed_passthrough():
    ''' Test that a class that inherits dict isn't improperly interpreted and
        modified.
        This is from Issue https://github.com/dask/dask/issues/2107

        NOTE : This doesn't work still, even after pull request.
            There is an issue between distributed and non distributed
    '''
    class MyClass(dict):
        pass

    @delayed(pure=True)
    def foo(arg):
        assert_true(isinstance(arg, MyClass))

    res = foo(MyClass())
    res.compute()

def test_calibration():
    # TODO : Replace with a portable db to to the db testing
    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires = source_dbtools.pull(dbname="cms:data", uid=uid)
    scires_data = SciResult(calibration=scires['attributes']).select("calibration")
    calib_protocol = LoadCalibration(calibration=scires_data)
    calibres = calib_protocol()
    assert isinstance(calibres, Delayed)
    calibres = calibres.compute()
    assert isinstance(calibres, SciResult)
    assert calibres.select("beamx0").get()['value'] == 379.0
    #print(calibres)

    calib_protocol = LoadCalibration()
    calib_protocol.set_keymap("None")
    calib_protocol.add(name='beamx0', value=50, unit='pixel')
    calib_protocol.add(name='beamy0', value=50, unit='pixel')
    calib_protocol.add(name='detectors', value=['pilatus300'], unit='N/A')
    calibres = calib_protocol().compute()
    assert calibres.select("beamx0").get()['value'] == 50
    #print(calibres)
    
def test_load_saxs_img(plot=False, output=False):
    ''' test the load_saxs_img class'''
    # I randomly chose some header
    # TODO : source should be managed in a function, need to think more about
    # it
    uid = '0b2a6007-ab1f-4f09-99ad-74c1c0a4b391'
    scires_header = source_dbtools.pull('cms:data', uid=uid)
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

    res_headerinput = img_load_protocol(infile=scires_header('pilatus300_image'), detector=detectors2D['pilatus300'])
    res_headerinput = res_headerinput





    if plot:
        import matplotlib.pyplot as plt
        plt.ion()
        plt.figure(0);plt.clf()
        plt.imshow(res_headerinput['image'])

    #return res_sciresinput

def test_circular_average(plot=False, output=False):

    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_header = source_dbtools.pull('cms:data', uid = uid)
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
    latest_calib = source_dbtools.pull('cms:analysis',  protocol_name="XS:calibration")
    #print(latest_calib)

    image_load_protocol = LoadSAXSImage()
    image = image_load_protocol(infile=scires_header, detector=detectors2D['pilatus300'], database='cms')

    circ_avg_protocol = CircularAverage()
    scires_circavg = circ_avg_protocol(image = image, calibration=scires_calib)
    scires_circavg = scires_circavg.compute()

    latest_calib = source_dbtools.pull('cms:analysis', protocol_name="XS:calibration")
    latest_circavg = source_dbtools.pull('cms:analysis', protocol_name="XS:circular_average")

    if output:
        return latest_circavg

def test_fit_peaks(plot=False, output=False):
    # SOURCE 1 : data
    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_data = source_dbtools.pull('cms:data', uid = uid)
    # select the output
    scires_data = scires_data('pilatus300_image')

    # SOURCE 1 : calibration data
    # set up calibration by reading from the attributes and filling in info
    # this could basically be anything, but here we want the attributes of a header as calibration
    scires_calibration = SciResult(calibration=scires_data['attributes'])
    scires_calibration = scires_calibration("calibration")

    load_calibration_protocol = LoadCalibration()


    # COMPUTATION 1 : loading some calibration data (basically re-mapping keys)
    # SINK 1 : Contains a sink to databroker "XS:Calibration"
    scires_calibration = load_calibration_protocol(calibration=scires_calibration)
    # Some of these can have multiple outputs, select the appropriate output
    scires_calibration = scires_calibration("calibration")

    # SOURCE 3 (optional) : Retrieving the latest calibration
    latest_calib = source_dbtools.pull('cms:analysis',  protocol_name="XS:calibration")

    load_saxs_image_protocol = LoadSAXSImage()
    # COMPUTATION 2 : load a SAXS image based on header data
    image = load_saxs_image_protocol(infile=scires_data, detector=detectors2D['pilatus300'], database='cms')

    circular_average_protocol = CircularAverage()
    # COMPUTATION 3 : compute the circular average
    # SINK 2 : Contains a sink to databroker "XS:circular_average"
    scires_circavg = circular_average_protocol(image=image, calibration=latest_calib)


    fit_peaks_protocol = FitPeaks()
    # COMPUTATION 4 : fit the peaks of a previous result
    # SINK 4 : Contains a sink to databroker "XS:fit_peak"
    scires_peaks = fit_peaks_protocol(sqx=scires_circavg('sqx'), sqy= scires_circavg('sqy'), npeaks=2)

    # FINAL STEP : compute the results (previously, all these computations were delayed
    scires_peaks = scires_peaks.compute()

    return scires_peaks

    # another test example, fitting known peaks with known guesses (for debugggin)
    #sqx = np.linspace(1e-3, .1, 1000)
    #sqy = np.exp(-(sqx-.02)**2/.001**2)+np.exp(-(sqx-.04)**2/.001**2)
    #scires_peaks = fitpeaks(sqx=sqx, sqy=sqy, npeaks=2, init_guess={"A0" : 1, "A1" : 1, "x00" : .02, "x01" : .04, "sigma0" : .001, "sigma1" : .001}).init().compute()


def test_future():
    @delayed(pure=True)
    def testfunc(a):
        return 1

    a = testfunc(1)
    future = _pipeline_client.compute(a)

    return future
