import tempfile
from PIL import Image
import os
import numpy as np

import time

from SciAnalysis.XSAnalysis.Data import Calibration

from databases import databases
from detectors import detectors2D


#from SciAnalysis.XSAnalysis.Protocols import load_SAXS_img

#import dask
from dask import delayed
#from distributed import Client
#_pipeline_client = Client('127.0.0.1:8788')

# caching stuff
#import chest
#_CACHE = chest.Chest()
#dask.set_options(cache=_CACHE)

# base class
class Protocol:
    pass

# TODO : add run_default
# TODO : add run_explicit
# TODO : how to handle implicit arguments? (Some global maybe?)

''' For now, assume all incoming arguments are well defined each step.

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



'''

class SciResult(dict):
    ''' Something to distinguish a dictionary from
        but in essence, it's just a dictionary.'''
    pass

# This decorator parses SciResult objects, indexes properly
# takes a keymap for args
def parse_args(keymap):
    # from keymap, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            for i, arg in enumerate(args):
                if isinstance(arg, SciResult):
                    key = keymap["arg{}".format(i)]
                    args[i] = arg[key]
            for key, val in kwargs.items():
                if isinstance(val, SciResult):
                    kwargs[key] = val[keymap[key]]
            return f(*args, **kwargs)
        return _f
    return decorator
    

class load_saxs_image:
    ''' Prototype to return SAXS Image'''
    # name
    _name = 'XS:load_saxs_image'
    # dependencies of the parameters
    _depends = dict()
    # explicit arguments for function
    # all other arguments assumed to just pass through
    _func_args = ['infile']
    # keymap for results
    _keymap = {'infile' : 'infile'}

    def __init__(self, infile=None):
        self.infile = infile

    @run_default
    def run(self):
        return self.run_explicit(infile=self.infile)

    @delayed(pure=True)
    @parse_args(_keymap)
    def run_explicit(infile=None):
        time.sleep(2)
        print("calculate")
        im = Image.open(infile)
        results = dict()
        results['image'] = np.array(im)
        return results

class load_calibration:
    _name = "XS:load_calibration"
    _depends = dict()
    # explicitly declaring the function's necessary arguments
    _func_args = ['db', 'calibration']

    # no parse results decorator for this for now
    # TODO : handle multiple detectors
    def __init__(self, header, calib_keymap):
        keymaps = calib_keymap

        # using keymap to extract data
        start_doc = header['start']
        wavelength_A = start_doc[keymaps['wavelength_A']]
        sample_det_distance_m = start_doc[keymaps['sample_det_distance_m']]
        beamx0 = start_doc[keymaps['beamx0']]
        beamy0 = start_doc[keymaps['beamy0']]
        # just take first detector
        first_detector = start_doc[keymaps['detectors']][0]
        detector_key = detectors2D[first_detector]['image_key']

        # look up in local library
        image_key = first_detector + "_image"
        pixel_size_um = detectors2D[first_detector]['xpixel_size_um']
        img_shape = detectors2D[first_detector]['shape']
        
        # now init calibration
        self.calibration = Calibration(wavelength_A=wavelength_A) # 13.5 keV
        self.calibration.set_image_size(img_shape[1], height=img_shape[0]) # Pilatus300k
        self.calibration.set_pixel_size(pixel_size_um=pixel_size_um)
        self.calibration.set_distance(sample_det_distance_m)
        self.calibration.set_beam_position(beamx0, beamy0)
      
   
    def run(self):
        return self.run_explicit(self.calibration)

    @delayed(pure=True)
    def run_explicit(calibration):
        results = SciResult(calibration=calibration)
        return results

class circular_average:
    ''' circular average.'''
    _name = 'XS:circular_average'
    # a dictionary for each argument, maps the processed result to the key in processed result
    _depends = {
                'image' : {'XS:load_saxs_image' : 'image'},
                'calibration' : {'XS:load_calibration' : 'calibration'},
               }
    _keymap = {'image' : 'image', 'calibration' : 'calibration'}

    def __init__(self, image=None, calibration=None):
        self.image = image
        self.calibration = calibration

    def run(self):
        return self.run_explicit(image=self.image, calibration=self.calibration)

    # TODO make sure RadialBinnedStatistic can take delayed objects
    @delayed(pure=True)
    @parse_args(_keymap)
    def run_explicit(image=None, calibration=None):
        print(calibration)
        x0, y0 = calibration.x0, calibration.y0
        from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic
        result = SciResult()
        result['sq'] = np.ones((100))
        return result

def test_load_saxs_img():
    tmpdir_data = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_data)

    # make dummy data
    img_shape = (100,100)
    data = np.ones(img_shape, dtype=np.uint8)
    data[50:60] = 0
    data_filename = tmpdir_data + "/test_data.png"
    data_filename = "/tmp/tmpw041itcd/test_data.png"
    im = Image.fromarray(data)
    im.save(data_filename)

    # testing that protocol can take a SciResult or data
    head = SciResult(infile=data_filename)
    res1 = load_saxs_image(infile=head)
    im1 = res1.run()
    res2 = load_saxs_image(infile=data_filename)
    im2 = res2.run()


    print(im1)
    print(im2)

    # testing function purity
    im1entry = im1['image']
    im2entry = im2['image']
    print(im1entry)
    print(im2entry)

    im1entryshape = im1entry.shape#delayed(getattr)(im1entry, 'shape', pure=True)
    im2entryshape = im2entry.shape#delayed(getattr)(im2entry, 'shape', pure=True)

    print(im1entryshape)
    print(im2entryshape)

    #res = _pipeline_client.submit(img1entryshape)
    print((im1entryshape[0] + im2entryshape[0]).compute())
    print(im1entryshape.compute())
    #print(im2entryshape.compute())


    
def test_circavg():
    cmsdb = databases['cms']['data']
    # I randomly chose some header
    header = cmsdb['89e8caf6-8059-43ff-9a9e-4bf461ee95b5']

    tmpdir_data = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_data)

    # make dummy data
    img_shape = (100,100)
    data = np.ones(img_shape, dtype=np.uint8)
    data[50:60] = 0
    data_filename = tmpdir_data + "/test_data.png"
    data_filename = "/tmp/tmpw041itcd/test_data.png"
    im = Image.fromarray(data)
    im.save(data_filename)

    # testing that protocol can take a SciResult or data
    head = SciResult(infile=data_filename)
    res1 = load_saxs_image(infile=head)
    im1 = res1.run()
    res2 = load_saxs_image(infile=data_filename)
    im2 = res2.run()
    calib_keymap = {'wavelength_A' : 'calibration_wavelength_A',
                    'detectors' : 'detectors',
                    'beamx0' : 'detector_SAXS_x0_pix',
                    'beamy0' : 'detector_SAXS_y0_pix',
                    'sample_det_distance_m' : 'detector_SAXS_distance_m'
                    }


    calibres = load_calibration(header, calib_keymap).run()
    image = load_saxs_image(infile=data_filename).run()
    sq = circular_average(image=image, calibration=calibres).run()

    return sq
