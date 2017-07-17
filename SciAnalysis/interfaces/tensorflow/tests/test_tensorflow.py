import numpy as np
import tempfile
from SciAnalysis.tools import make_dir

from SciAnalysis.interfaces.tensorflow.tensorflow \
        import store_result_tensorflow
from SciAnalysis.config import TFLAGS


def test_store_result():
    result = dict()
    result['kwargs'] = {'image': np.ones((100, 100))}

    # temp path
    tmp_path = tempfile.mktemp()
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
    arr = np.fromfile(filename, dtype=np.uint32)

    arr = arr.reshape((-1, res[2], res[3]))

    assert arr.shape[0] == 1
