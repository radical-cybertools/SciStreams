import numpy as np
import tempfile

from SciStreams.utils import make_dir

from SciStreams.interfaces.tensorflow.tensorflow \
        import store_result_tensorflow, read_result_tensorflow,\
        get_filenames,\
        label2bin, bin2label, calc_labelbin_size

from SciStreams.config import TFLAGS


def _test_store_result(image, labels=None):
    result = dict()
    result['kwargs'] = {'image': image}
    if labels is not None:
        result['kwargs']['labels'] = labels

    # temp path
    tmp_path = tempfile.mktemp(prefix="mlpipeline")
    make_dir(tmp_path)
    TFLAGS.data_dir = tmp_path
    TFLAGS.num_per_batch = 3

    store_result_tensorflow(result, dataset="test")
    store_result_tensorflow(result, dataset="test")
    store_result_tensorflow(result, dataset="test")
    store_result_tensorflow(result, dataset="test")
    # print("saved in {}".format(tmp_path))

    # test the read worked
    filename = tmp_path + "/test/master_file.txt".format(0)
    res = np.loadtxt(filename, delimiter=" ", dtype=np.uint32)
    # print(res)
    # test num batches is 1 (so on second batch)
    assert res[0] == 1
    # test that the num per batch is 3, as was set
    assert res[1] == TFLAGS.num_per_batch
    filename = tmp_path + "/test/{:08d}.bin".format(1)
    record = read_result_tensorflow(0, dataset="test")

    image_shape = res[2], res[3]

    # leaving it here to be sure this numpy assumption still holds
    # nbytes = np.uint32().nbytes
    # num_labels = res[4]
    # elems_per_label = calc_labelbin_size(num_labels, nbytes)
    # labels = arr[:elems_per_label]
    # image = arr[elems_per_label:]
    labels = record.labels
    image = record.image
    image_size = image.shape[0]*image.shape[1]
    assert image_size == image_shape[0]*image_shape[1]
    # should reshape fine
    image = image.reshape(image_shape)

    fnames = get_filenames(dataset="test", fpath=tmp_path)
    assert fnames[1] == tmp_path + "/test/{:08d}.bin".format(0)


def test_store_results():
    image = np.ones((100, 100))
    _test_store_result(image, labels=None)
    labels = [True, True, False, False, True]
    _test_store_result(image, labels=labels)


def test_label2bin():
    ''' test the labels to bin format.'''
    # labels read left to right
    labels = [True]*9
    num_labels = len(labels)
    res = label2bin(labels, dtype=np.uint8)
    assert (res == np.array([255, 1])).all()

    labels = [True, 0, True, True]
    num_labels = len(labels)
    res = label2bin(labels, dtype=np.uint8)
    assert (res == np.array([13])).all()

    # check transformation works
    returned_labels = bin2label(res, num_labels)
    assert all([label == returned_label
                for label, returned_label
                in zip(labels, returned_labels)])

    # now check for integers
    labels = [2, 0, 2, 1, 1]
    num_labels = len(labels)
    res = label2bin(labels, dtype=np.uint8)
    assert (res == np.array([29])).all()


def test_calc_labelbinsize():
    assert calc_labelbin_size(0, 0) == 0
    assert calc_labelbin_size(0, 1) == 0

    # these should not gi
    assert calc_labelbin_size(8, 1) == 1
    assert calc_labelbin_size(9, 1) == 2
    assert calc_labelbin_size(9, 2) == 1
