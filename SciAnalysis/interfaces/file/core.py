# core functions that are not classes
# classes in this hierarchy will inherit them etc
# stuff for reading/writing files
from PIL import Image
import h5py
import numpy as np


def npywriter(data=None, filename=None):
    if data is None:
        raise ValueError("Error, data not specified")
    if isinstance(data, dict):
        filename = filename + ".npz"
        np.savez(filename, **data)
    elif isisntance(data, np.ndarray):
        filename = filename + ".npy"
        np.save(filename, data)
    else:
        filename = filename + ".npz"
        np.savez(filename, data=data)

def datwriter(data=None, filename=None):
    if data is None:
        raise ValueError("Error, data not specified")
    if isinstance(data, dict):
        filename = filename + ".dat"
        data_keys = list(data.keys())
        nitems = len(data_keys)
        data_shape = data[data_keys[0]].shape
        res = np.zeros((*data_shape, nitems))
        header = ""
        for i, key in enumerate(data_keys):
            header = header + key + "\t"
            res[:,i] = data[key]
        np.savetxt(filename, res, delimiter=" ", header=header)
    else:
        raise ValueError("Should be a dictionary")

def pngwriter(data=None, filename=None):
    filename = filename + ".png"
    if data is None:
        raise ValueError("Error, data not specified")
    # if it's a dict, get entry
    if isinstance(data, dict):
        keys = list(data.keys())
        if len(keys) > 1:
            raise ValueError("can't write a png with more than one elem in dict")
        data = data[keys[0]]
    data = Image.fromarray(data.astype(np.uint8))
    print(filename)
    data.save(filename)

def jpegwriter(data=None, filename=None):
    filename = filename + ".jpg"
    if data is None:
        raise ValueError("Error, data not specified")
    # if it's a dict, get entry
    if isinstance(data, dict):
        keys = list(data.keys())
        if len(keys) > 1:
            raise ValueError("can't write a png with more than one elem in dict")
        data = data[keys[0]]
    data = Image.fromarray(data.astype(np.uint8))
    print(filename)
    data.save(filename)

def jpegloader(filename=None):
    if filename is None:
        raise ValueError("Did not receive a filename")

    data = Image.open(filename)
    data = np.asarray(data)
    return data

def pngloader(filename=None):
    return jpegloader(filename=filename)

def hdf5loader(filename=None, entryname=None):
    if filename is None:
        raise ValueError("Did not receive a filename")
    if entryname is None:
        raise ValueError("Did not receive an entry name for hdf5 file")
    with h5py.File(filename, 'r') as f:
        data = np.asarray(f[entryname])
    return data

def npyloader(filename=None):
    res = np.load(filename)
    return dict(res)

writers_dict = {'npy' : npywriter, 'jpg' : jpegwriter, 'png' : pngwriter, 'dat' : datwriter}
