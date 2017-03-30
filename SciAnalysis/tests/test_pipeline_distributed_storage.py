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

def store_results(dbname, external_writers={}):
    def decorator(f):
        def newf(*args, **kwargs):
            import SciAnalysis.interfaces.databroker.dbtools as source_dbtools
            results = f(*args, **kwargs)
            # TODO : fill in (after working on xml storage)
            attributes = {}
            source_dbtools.store_results_databroker(results, dbname, external_writers=external_writers)
            return results
        return newf
    return decorator

    
# interface side stuff
class load_saxs_image:
    _keymap = {'infile' : 'infile'}
    _output_names = ['image']
    _name = "XS:load_saxs_image"
    _attributes = {}
    _dbname = 'cms'

    def __init__(self, **kwargs):
        self.kwargs= kwargs

    def init(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(_name=self._name, **new_kwargs)

    # need **kwargs to allow extra args to be passed
    @delayed(pure=False)
    #@store_results('cms')
    @run_default
    @parse_sciresults(_keymap, _output_names, _attributes)
    def run(infile=None, **kwargs):
        import SciAnalysis.interfaces.databroker.dbtools as source_dbtools
        if isinstance(infile, np.ndarray):
            img = infile
        elif isinstance(infile, str):
            img = np.array(Image.open(infile))
        else:
            raise ValueError("Sorry, did not understand the input argument: {}".format(infile))

        return img


class load_cms_calibration:
    # TODO: re-es = ['calibration']
    _keymap = {'calibration' : 'calibration'}
    _output_names = ['calibration']
    _name = "XS:calibration"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def init(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(_name=self._name, **new_kwargs)

    def add(self, name=None, value=None, unit=None):
        self.kwargs.update({name : {'value' : value, 'unit' : unit}})

    @delayed(pure=True)
    @store_results('cms')
    @run_default
    @parse_sciresults(_keymap, _output_names)
    def run(calibration={}, **kwargs):
        '''
            Load calibration data from a SciResult's attributes.

            The data must be a dictionary.
            either:
                load_calibration(calibration=myCalib)
            or:
                load_calibration(wavelength=fdsa) etc
            
            This is area detector specific.
        '''
        # defaults of function
        _defaults= {'wavelength' : {'value' : None, 'unit' : 'Angstrom'},
                     'beamx0' : {'value' : None, 'unit' : 'pixel'},
                     'beamy0' : {'value' : None, 'unit' : 'pixel'},
                     'sample_det_distance' : {'value' : None, 'unit' : 'm'},
                    # Area detector specific entries:
                     # width is columns, height is rows
                     'AD_width' : {'value' : None, 'unit' : 'pixel'},
                     'AD_height' : {'value' : None, 'unit' : 'pixel'},
                     'pixel_size_x' : {'value' : None, 'unit' : 'pixel'},
                     'pixel_size_y' : {'value' : None, 'unit' : 'pixel'},
                       #TODO : This assumes data has this detector, not good to use, remove eventually
                     'detectors' : {'value' : ['pilatus300'], 'unit' : None},
                    }
    
        # a map from Header start doc to data
        # TODO : move out of function
        calib_keymap = {'wavelength' : {'key' : 'calibration_wavelength_A',
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

        # update calibration with all keyword arguments
        for key, val in kwargs.items():
            # make sure not a hidden parameter
            if not key.startswith("_") and key not in calibration:
                calibration[key] = _defaults[key]
        # now override with kwargs
        for key in _defaults.keys():
            if key in kwargs:
                calibration[key] = kwargs[key]

        calib_tmp = dict()
        # walk through defaults
        for key, entry in calib_keymap.items():
            start_key = entry['key'] # get name of key
            unit = entry['unit']
            val = calibration.get(start_key, _defaults[key]['value'])
            calib_tmp[key] = {'value' : val,
                              'unit' : unit}

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
        
    
        return calibration
        

class circular_average:
    _keymap = {'calibration': 'calibration', 'image' : 'image'}
    _output_names = ['sqx', 'sqy']
    _name = "XS:circular_average"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def init(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(_name=self._name, **new_kwargs)

    @delayed(pure=True)
    @store_results('cms', {'sqx' : 'npy', 'sqy' : 'npy'})
    @run_default
    @parse_sciresults(_keymap, _output_names)
    def run(image=None, calibration=None, bins=100, mask=None, **kwargs):
        #print(calibration)
        #print("computing")
        x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']
        from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic
        img_shape = calibration['shape']['value']
        #print(img_shape)
        rbinstat = RadialBinnedStatistic(img_shape, bins=bins, origin=(y0,x0), mask=mask)
        sq = rbinstat(image)
        sqx = rbinstat.bin_centers

        return sqx, sq

class fitpeaks:
    _keymap = {'sqx': 'sqx', 'sqy' : 'sqy', 'q0' : 'q0'}
    _output_names = ['fit_params', 'fit_stats', 'best_fit']
    _name = "XS:fitpeaks"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def init(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(_name=self._name, **new_kwargs)

    @delayed(pure=True)
    @store_results('cms', {'best_fit' : 'npy'})
    @run_default
    @parse_sciresults(_keymap, _output_names)
    def run(sqx=None, sqy=None, init_guess=None, ncurves=None, **kwargs):
        if sqx is None or sqy is None:
            raise ValueError("sqx, sqy or q0 not defined")
        if ncurves is None:
            ncurves = 1

        if init_guess is None:
            init_guess = {}

        import lmfit
        params = lmfit.Parameters()
        for i in range(ncurves):
            key = "A-{}".format(i)
            key = "x0-{}".format(i)
            key = "sigma-{}".format(i)

            params.add(key, value=1.)
            params.add(key.format(i), value=0.)
            params.add(key.format(i), value=0.1)
            # update with init_guess if exists
            if key in init_guess:
                params[key].value = init_guess[key]

        def gausspeaks(x, npeaks, params):
            res = np.zeros_like(x)
            for i in range(npeaks):
                amp = params["A-{}".format(i)]
                x0 = params["x0-{}".format(i)]
                sigma = params["sigma-{}".format(i)]
                res += amp*np.exp(-(x-x0)**2/2./sigma**2)
            return res

        def minimizer(params, ncurves, sqx, sqy):
            params = params.valuesdict()
            res = gausspeaks(sqx, ncurves, params)
            return np.sum((res-sqy)**2)

        lmfit.minimize(gausspeaks, params, args=(ncurves, sqx, sqy))

        return sqx, sq




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
    @parse_sciresults({'a' : 'a'}, 'a')
    def foo(a=1, **kwargs):
        return a

    test = SciResult(a=1)

    res = foo(a=test)
    assert res.get('a') == 1


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
    calibres = load_cms_calibration(calibration=scires_data).init()
    assert isinstance(calibres, Delayed)
    calibres = calibres.compute()
    assert isinstance(calibres, SciResult)
    print(calibres)

    calibres = load_cms_calibration()
    calibres.add(name='beamx0', value=50, unit='pixel')
    calibres.add(name='beamy0', value=50, unit='pixel')
    calibres.add(name='detectors', value=['pilatus300'], unit='N/A')
    calibres = calibres.init().compute()
    print(calibres)
    
def test_load_saxs_img(plot=False, output=False):
    ''' test the load_saxs_img class'''
    # I randomly chose some header
    # TODO : source should be managed in a function, need to think more about
    # it
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_header = source_dbtools.pull('cms:data', uid=uid)
    scires_header = scires_header.select('pilatus300_image')

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
    res_fileinput = load_saxs_image(infile=scires_datafile).init()
    # test with sciresult
    #print(scires_header)
    # TODO : try adding attributes
    #sample_name = scires_header['attributes']['sample_name']
    #sample_uid = scires_header['attributes']['uid']

    res_headerinput = load_saxs_image(infile=scires_header, detector=detectors2D['pilatus300']).init()

    #assert_true(isinstance(res_fileinput, Delayed))
    #assert_true(isinstance(res_headerinput, Delayed))

    # test with data
    #res_fileinput = res_fileinput.compute()
    # test with sciresult
    res_headerinput = res_headerinput.compute()

    #assert_array_almost_equal(data, res_fileinput['outputs']['image'])

    if plot:
        import matplotlib.pyplot as plt
        plt.ion()
        plt.figure(0);plt.clf()
        plt.imshow(res_headerinput['image'])

    #return res_sciresinput

def test_circular_average(plot=False, output=False):

    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_data = source_dbtools.pull('cms:data', uid = uid)
    scires_data = SciResult(calibration=scires_data['attributes']).select('calibration')

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
    calibres = load_cms_calibration(calibration=scires_data).init().compute()
    # retrieve the latest calibration
    latest_calib = source_dbtools.pull('cms:analysis',  protocol_name="XS:calibration")
    #print(latest_calib)

    image = load_saxs_image(infile=scires_data, detector=detectors2D['pilatus300'], database='cms').init()

    latest_calib = source_dbtools.pull('cms:analysis', protocol_name="XS:calibration")
    latest_circavg = source_dbtools.pull('cms:analysis', protocol_name="XS:circular_average")

    if output:
        return sqres

def test_fit_peaks(plot=False, output=False):
    # I randomly chose some header
    uid = '89e8caf6-8059-43ff-9a9e-4bf461ee95b5'
    scires_data = source_dbtools.pull('cms:data', uid = uid)

    # set up calibration by reading from the attributes and filling in info
    scires_calibration= SciResult(calibration=scires_data['attributes']).select('calibration')
    #calibres = load_calibration(calibration=header).run()
    calibres = load_cms_calibration(calibration=scires_calibration).init().compute()
    # retrieve the latest calibration
    latest_calib = source_dbtools.pull('cms:analysis',  protocol_name="XS:calibration")
    #print(latest_calib)

    #image = load_saxs_image(infile=scires_data, detector=detectors2D['pilatus300'], database='cms').init().compute()

    #scires_circavg = circular_average(image=image, calibration=latest_calib).init()

    #scires_circavg.compute()


    #latest_circavg = source_dbtools.pull('cms:analysis', protocol_name="XS:circular_average")

    if output:
        return sqres
