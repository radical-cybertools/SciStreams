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

from SciAnalysis.SciResult import SciResult
from SciAnalysis.decorators import parse_sciresults
from numpy.testing import assert_array_almost_equal
 

from Protocol import Protocol

# transform function to class
@Protocol(name="XS:load_saxs_image", 
          output_names=["image"], keymap={"infile" : "infile"},
          accepted_args=["infile", 'detector', 'database'])
# Now define the function, make sure arguments make operation uniquely defined
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
    elif isinstance(infile, SciResult):
        raise ValueError("Error, got a SciResult. Make sure your keymap is well defined.")
    else:
        raise ValueError("Sorry, did not understand the input argument: {}".format(infile))
    
    return img

# test addition of bound methods (should eventually be decorator)
class load_saxs_image(load_saxs_image):
    def foo(self):
        pass


def test_load_saxs_img(plot=False, output=False):
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

    if output:
        return res_headerinput['image']

