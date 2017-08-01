# stuff involving writing to data that will get sent to tensorflow
from ...config import TFLAGS
from ...tools import make_dir
import os

import numpy as np


class MasterRecord:
    ''' This is the object that defines a set of records.'''

    allowed_kwargs = ['number_records', 'number_batches',
                      'image_shape', 'number_labels']

    def __init__(self, **kwargs):
        for key in self.allowed_kwargs:
            if key in kwargs:
                setattr(self, key, kwargs[key])


class Record:
    ''' This is the object that defines a set of records.'''

    allowed_kwargs = ['image', 'labels']

    def __init__(self, **kwargs):
        for key in self.allowed_kwargs:
            if key in kwargs:
                setattr(self, key, kwargs[key])


def calc_labelbin_size(num_labels, nbytes):
    ''' Calculate the number of elements needed for the label binary.

        Parameters
        ----------

        num_labels : int
            number of labels

        nbytes : int
            the number of bytes per element

        Returns
        -------

        The number of elements necessary to store the labels.
    '''

    if num_labels == 0:
        return 0

    nbits = nbytes*8

    return (num_labels + nbits - 1)//nbits


def label2bin(labels, dtype=np.uint8):
    ''' Takes a binary list of labels and puts it in a bit array.
        Each bit represents True (1) / False (0) for the label.

        Parameters
        ----------

        labels : list of bools
            a list of labels
            if not a list of bools, 0 is assumed False
                and any nonzero entry True

        dtype : type, optional
            the data type of array

        Returns
        -------

        result : np.ndarray
            the resultant binary array

        Notes
        -----

        The labels read from lowest index to highest index in the list.

        The elements read from least significant to most significant bit in
        each 8-bit chunk, and then from lowest index to highest index in the
        array of 8-bit chunks.

        Be careful when writing this to disk as endianness may change from
        machine to machine. Using np.save is endian friendly, but np.tofile is
        not.
    '''
    nbytes_per_type = dtype().nbytes
    nbits_per_type = nbytes_per_type*8

    num_labels = len(labels)
    nbytes = calc_labelbin_size(num_labels, nbytes_per_type)

    result = np.zeros(nbytes, dtype=dtype)
    for i, label in enumerate(labels):
        result[i//nbits_per_type] |= (label > 0) << (i % nbits_per_type)

    return result


def bin2label(res, num_labels):
    ''' Transform a bit array to a list of binary labels
        Reverse transformation of label2bin.

        Parameters
        ----------

        res : np.ndarray
            the binary array

        num_labels : list
            the list of labels
    '''
    dtype = res.dtype.type
    nbytes_per_type = dtype().nbytes
    nbits_per_type = nbytes_per_type*8
    labels = np.zeros(num_labels, dtype=int)

    for i in range(num_labels):
        labels[i] = ((res[i//nbits_per_type] &
                      (1 << i % nbits_per_type)) >>
                     (i % nbits_per_type)) > 0

    return labels


def store_result_tensorflow(result, dataset=None, dtype=np.uint32, num_per_batch=None):
    ''' Store an image to be read by tensorflow
        This prepares binary files in temporary directories to be read by
        tensorflow applications.


        This follows the CIFAR-10 format [1], except assumes 32 bit unsigned
        integers (np.uint32) per pixel

        Parameters
        ----------

        result: StreamDoc
            Assumes a StreamDoc with:
                'image' : np.ndarray
                    The image. This only takes one image at a time for now.

                'labels' : ordered list of bools, optional
                    if present, also saves labels. Must be an ordered list of
                    bools. If integers, 0 is counted as False and the rest
                    true.
                    If labels not present or an empty list, this is assumed
                    unlabeled data.


        dataset : string
            the dataset name. This is required.

        dtype : type
            must be a numpy array data type (must have an nbytes attribute)
            this is the size of each element in the array being read

        Notes
        -----

        Unlabeled and labeled data cannot be mixed in a data set.

        Endianness is lost with this method (following what CIFAR-10 does, so
        we can use their record reader).  Do not run this across machines with
        different endianness (this storage is not meant for archiving)

        There is no file locking mechanism here. Make sure this code running
        does not have competing access or else this may result in inconsistent
        behaviour.

        The data relies on a master file to describe it. Its format is:
            num recs, num batches per rec, image shape 1, image shape 2,
                num labels
            where :
                num recs : number of records
                num batches per rec : number of batches per record
                image shape 1/2 : image shape
                num labels : number of labels
                    If this is zero, then unlabeled data assumed

        References
        ---------

        [1] https://raw.githubusercontent.com/tensorflow/models/
                master/tutorials/image/cifar10/cifar10_multi_gpu_train.py
    '''
    if num_per_batch is None:
        num_per_batch = TFLAGS.num_per_batch

    if dataset is None:
        raise ValueError("Error dataset not supplied")

    # grab the image from StreamDoc
    kwargs = result['kwargs']
    image = kwargs['image']

    if 'labels' in kwargs:
        labels = kwargs['labels']
        num_labels = len(labels)
        labelbin = label2bin(labels, dtype=dtype)
    else:
        labels = None
        num_labels = 0
        labelbin = np.array([], dtype=dtype)

    nbytes = dtype().nbytes
    num_elems_label = calc_labelbin_size(num_labels, nbytes)

    # check image is a numpy array
    if not isinstance(image, np.ndarray):
        image = np.array(image, dtype=dtype)

    if image.ndim != 2:
        errormsg = "Must be an image of 2 dimensions (not a stack)"
        errormsg += "\n Received {} dimensions".format(image.ndim)
        raise ValueError(errormsg)

    # force it to have the required data type
    image = image.astype(dtype)

    # make image and label data
    data = np.concatenate((labelbin, image.ravel()))
    expected_size = image.shape[0]*image.shape[1] + num_elems_label
    if (len(data) != expected_size):
        errormsg = "Data length doesn't match labels and image size\n"
        errormsg += "Got {}\n".format(len(data))
        errormsg += "Expected {}\n".format(expected_size)
        raise ValueError(errormsg)

    elems_per_data = len(data)

    # TODO : add file lock
    # TODO : make master file that saves different data sets
    fpath = TFLAGS.data_dir + "/" + dataset
    make_dir(fpath)

    master_filename = fpath + "/master_file.txt"
    # create master file if not present
    if not os.path.isfile(master_filename):
        # num recs, num batches per rec, image shape 1, image shape 2,
        #      num labels
        # TODO : add data type to master header
        _save_master_file(master_filename, 0, num_per_batch,
                          image.shape, num_labels)

    master_record = _read_master_file(master_filename)
    # res = np.loadtxt(master_filename, delimiter=" ", dtype=dtype)

    numrecs = master_record.number_records
    # numbatches = master_record.records_per_batch
    # image_shape = master_record.image_shape
    num_labels = master_record.number_labels

    # elems_per_label = calc_labelbin_size(num_labels, dtype().nbytes)

    if (num_labels == 0 and (labels is not None and len(labels) != 0)) \
       or (labels is not None and (num_labels != len(labels))):
        if labels is None:
            actual_labels = 0
        else:
            actual_labels = len(labels)

        errormsg = "Number of labels doesn't match labels\n"
        errormsg += "Expected {} but got {}".format(num_labels, actual_labels)
        raise ValueError(errormsg)

    curfilename = fpath + "/{:08d}.bin".format(numrecs)

    # initial
    if not os.path.isfile(curfilename):
        arr = data
    else:
        # read curfile as raw array
        # arr = np.fromfile(curfilename, dtype=dtype)
        arr = _read_batch(numrecs, dataset=dataset, dtype=dtype)
        array_size = len(arr)
        if array_size % elems_per_data != 0:
            errormsg = "Data mismatch\n"
            errormsg += "array size : {}\n".format(array_size)
            errormsg += "elems per data : {}\n".format(elems_per_data)
            errormsg += "array size should be divisible by elems per data"
            raise ValueError(errormsg)

        num_elements = array_size/elems_per_data

        # if the number of elements has reached maximum batch size
        if num_elements == num_per_batch:
            # update num recs and current file
            numrecs += 1
            _save_master_file(master_filename, numrecs, num_per_batch,
                              image.shape, num_labels)
            curfilename = fpath + "/{:08d}.bin".format(numrecs)
            # arr = np.fromfile(curfilename, dtype=dtype).reshape((-1,
            # image_shape[0], image_shape[1]))
            arr = data
        else:
            arr = np.concatenate((arr, data)).astype(dtype)

    arr.tofile(curfilename)

    # now save image as a batch into a file


def read_result_tensorflow(recno, dataset=None, dtype=np.uint32):
    record = _read_record(recno, dataset=dataset, dtype=dtype)
    return record


def _read_batch(batch_number, dataset=None, dtype=np.uint32):
    ''' Read a batch result. This is raw data (with labels if present).'''
    fpath = TFLAGS.data_dir + "/" + dataset

    batch_filename = fpath + "/{:08d}.bin".format(batch_number)
    arr = np.fromfile(batch_filename, dtype=dtype)

    return arr


def _read_record(record_number, dataset=None, dtype=np.uint32):
    ''' read a record
    Save the master file
    '''
    fpath = TFLAGS.data_dir
    master_filename = fpath + "/" + dataset + "/master_file.txt"
    # first read the master
    master_record = _read_master_file(master_filename)

    number_records = master_record.number_records
    records_per_batch = master_record.records_per_batch
    img_shape = master_record.image_shape
    num_labels = master_record.number_labels

    if record_number > number_records:
        errormsg = "Error, only {} records available, ".format(number_records)
        errormsg += "but asked for record # {}".format(record_number)
        raise ValueError(errormsg)

    batch_number = record_number//records_per_batch
    batch_record = record_number % records_per_batch

    arr = _read_batch(batch_number, dataset=dataset, dtype=dtype)

    # size in elements not bytes
    label_size = calc_labelbin_size(num_labels, dtype().nbytes)
    image_size = img_shape[0]*img_shape[1]
    record_size = image_size + label_size

    record = arr[batch_record*record_size:(batch_record + 1)*record_size]
    labels = bin2label(record[:label_size], num_labels)
    image = record[label_size:].reshape(img_shape)
    record = Record(labels=labels, image=image)

    return record


def _save_master_file(filename, num_recs, num_per_batch, image_shape,
                      num_labels):
    ''' Save the master file
        overwrites previous

        Format:
        num recs, num batches per rec, image shape 1, image shape 2,
            num labels
    '''
    np.savetxt(filename,
               [[num_recs, num_per_batch, image_shape[0], image_shape[1],
                 num_labels]],
               delimiter=" ", fmt=['%d', '%d', '%d', '%d', '%d'])


def _read_master_file(filename):

    res = np.loadtxt(filename, delimiter=" ", dtype=int)

    master_record = MasterRecord()
    master_record.number_records = res[0]
    master_record.records_per_batch = res[1]
    master_record.image_shape = res[2], res[3]
    master_record.number_labels = res[4]

    return master_record


def get_filenames(dataset=None, fpath=None):
    if fpath is None:
        fpath = TFLAGS.data_dir

    fpath = fpath + "/" + dataset

    fnames = [fpath + "/" + fname
              for fname in os.listdir(fpath)
              if 'bin' in fname]

    return fnames
