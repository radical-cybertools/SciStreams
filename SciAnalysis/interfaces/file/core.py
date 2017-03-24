# core functions that are not classes
# classes in this hierarchy will inherit them etc
# stuff for reading/writing files
from PIL import Image
import h5py
import numpy as np

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
