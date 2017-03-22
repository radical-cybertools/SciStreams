from dask import delayed
from dask.delayed import Delayed

from databroker.broker import Header

from SciAnalysis.contrib.decorators import simple_func2classdecorator
from SciAnalysis.databases import databases
import tempfile
import os
import numpy as np
from PIL import Image
from SciAnalysis.detectors import detectors2D

from SciResult import SciResult, parse_sciresults
from numpy.testing import assert_array_almost_equal
 
def Protocol(name="", output_names=list(), keymap = dict(), accepted_args=list()):
    @simple_func2classdecorator
    def decorator(f):
        class MyClass:
            _accepted_args = accepted_args
            _keymap = keymap
            _output_names = output_names
            _name = name
    
            def __init__(self, **kwargs):
                    self.kwargs = kwargs
        
            def run(self, **kwargs):
                new_kwargs = self.kwargs.copy()
                new_kwargs.update(kwargs)
                return self.run_explicit(_accepted_args=self._accepted_args, _name=self._name, **new_kwargs)
        
            @delayed(pure=True)
            @parse_sciresults(_keymap, _output_names)
            # need **kwargs to allow extra args to be passed
            def run_explicit(*args, **kwargs):
                # only pass accepted args
                new_kwargs = dict()
                for key in kwargs['_accepted_args']:
                    if key in kwargs:
                        new_kwargs[key] = kwargs[key]
                return f(*args, **new_kwargs)
    
        return MyClass
    return decorator

def foo(self, g):
    print("foo")

@NewClassMethod(foo)
# transform function to class
@Protocol(name="XS:load_saxs_image", 
          output_names=["image"], keymap={"infile" : "infile"},
          accepted_args=["infile", 'detector', 'database'])
# Now define the function
def load_saxs_image(infile=None, **kwargs):
    if isinstance(infile, Header):
        if 'detector' not in kwargs:
            raise ValueError("Sorry, detector must be passed if supplying a header")
        if 'database' not in kwargs:
            raise ValueError("Sorry, database must be passed if supplying a header")
        detector = kwargs.pop('detector')
        database = kwargs.pop('database')
        img = database.get_images(infile, detector['image_key']['value'])[0]
        img = np.array(img)
    elif isinstance(infile, np.ndarray):
        img = infile
    elif isinstance(infile, str):
        img = np.array(Image.open(infile))
    else:
        raise ValueError("Sorry, did not understand the input argument: {}".format(infile))
    
    return img


def test_load_saxs_img(plot=False):
    ''' test the load_saxs_img class'''
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
    im = Image.fromarray(data)
    im.save(data_filename)

    # testing that protocol can take a SciResult or data
    # test with data
    res_fileinput = load_saxs_image(infile=data_filename).run()
    # test with sciresult
    head = SciResult(infile=data_filename)
    res_sciresinput = load_saxs_image(infile=head).run()

    res_headerinput = load_saxs_image(infile=header, detector=detectors2D['pilatus300'], database=cmsdb).run()

    assert (isinstance(res_sciresinput, Delayed))
    assert (isinstance(res_fileinput, Delayed))
    assert (isinstance(res_headerinput, Delayed))

    # test with data
    res_fileinput = res_fileinput.compute()
    # test with sciresult
    res_sciresinput = res_sciresinput.compute()
    res_headerinput = res_headerinput.compute()

    assert_array_almost_equal(data, res_fileinput['image'])
    assert_array_almost_equal(data, res_sciresinput['image'])

    if plot:
        import matplotlib.pyplot as plt
        plt.ion()
        plt.figure(0);plt.clf()
        plt.imshow(res_headerinput['image'])

