# stuff involving writing to data that will get sent to tensorflow
from ...config import TFLAGS
from ...tools import make_dir
import os

import numpy as np


def store_result_tensorflow(result, dataset=None, dtype=np.uint32):
    ''' store an image
        Assumes a StreamDoc with 'image' present.

        This prepares binary files in temporary directories to be read by
        tensorflow applications.

        This follows the CIFAR-10 format, except assumes pixels of size uint32

        NOTE : endianness is lost with this method (following what CIFAR-10
            does, so we can use their record reader).
            Do not run this across machines with different endianness
            (this storage is not meant for archiving)
    '''
    if dataset is None:
        raise ValueError("Error dataset not supplied")

    # grab the image from StreamDoc
    kwargs = result['kwargs']
    image = kwargs['image']

    # check image is a numpy array
    if not isinstance(image, np.ndarray):
        image = np.array(image, dtype=dtype)

    image = image.astype(dtype)

    # check if dimensions are 3 or 2
    if image.ndim == 2:
        image = image.reshape((1, image.shape[0], image.shape[1]))

    # TODO : file lock
    # TODO : make master file that saves different data sets
    fpath = TFLAGS.data_dir + "/" + dataset
    make_dir(fpath)

    master_filename = fpath + "/master_file.txt"
    if not os.path.isfile(master_filename):
        # num recs num batches per rec, image shape 1, image shape 2
        np.savetxt(master_filename,
                   [[0, TFLAGS.num_per_batch, image.shape[1], image.shape[2]]],
                   delimiter=" ", fmt=['%d', '%d', '%d', '%d'])

    res = np.loadtxt(master_filename, delimiter=" ", dtype=dtype)

    numrecs = res[0]
    # numbatches = res[1]
    image_shape = res[2], res[3]

    curfilename = fpath + "/{:08d}.bin".format(numrecs)

    # initial
    if not os.path.isfile(curfilename):
        arr = image.astype(dtype)
    else:
        # read curfile
        arr = np.fromfile(curfilename, dtype=dtype)\
                .reshape((-1, image_shape[0], image_shape[1]))
        # check if filles
        if arr.shape[0] == TFLAGS.num_per_batch:
            # update num recs and current file
            numrecs += 1
            np.savetxt(master_filename,
                       [[numrecs, TFLAGS.num_per_batch, image.shape[1],
                         image.shape[2]]],
                       delimiter=" ", fmt=['%d', '%d', '%d', '%d'])
            curfilename = fpath + "/{:08d}.bin".format(numrecs)
            # arr = np.fromfile(curfilename, dtype=dtype).reshape((-1,
            # image_shape[0], image_shape[1]))
            arr = image
        else:
            arr = np.concatenate((arr, image), axis=0).astype(dtype)

    print(arr.shape)

    arr.tofile(curfilename)

    # now save image as a batch into a file
