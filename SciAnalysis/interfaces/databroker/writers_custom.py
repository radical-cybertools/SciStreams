# TODO : change this to some global analysis directory
# and make sure it works in a GLOBAL filesystem for distributed environment
_ROOTDIR = "/home/lhermitte/sqlite/fs-tmp-data"
import os
import uuid
import numpy as np
from PIL import Image

class NpyWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """

    SPEC = 'npy'

    def __init__(self, fs, root=_ROOTDIR):
        ''' the rood directory can be overwritten.'''
        self._root = root
        self._closed = False
        self._fs = fs
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data, subpath=""):
        """
        Save data to file, generate and insert new resource and datum.
        """
        if self._closed:
            raise RuntimeError('This writer has been closed.')

        dirpath = '{}{}'.format(self._root, subpath)
        fp = '{}/{}.npy'.format(dirpath, str(uuid.uuid4()))
        # check it exists and make
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        np.save(fp, data)
        resource = self._fs.insert_resource(self.SPEC, fp, resource_kwargs={})
        datum_id = str(uuid.uuid4())
        self._fs.insert_datum(resource=resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        self._closed = True

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class PILWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    Writer using PIL library
    """
    def __init__(self, fs, root=_ROOTDIR):
        ''' the rood directory can be overwritten.'''
        self._root = root
        self._closed = False
        self._fs = fs
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data, subpath=""):
        """
        Save data to file, generate and insert new resource and datum.
        """
        if self._closed:
            raise RuntimeError('This writer has been closed.')

        dirpath = '{}{}'.format(self._root, subpath)
        fp = '{}/{}.{}'.format(dirpath, str(uuid.uuid4()), self.ext)
        # check it exists and make
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        Image.fromarray(data).save(fp)
        resource = self._fs.insert_resource(self.SPEC, fp, resource_kwargs={})
        datum_id = str(uuid.uuid4())
        self._fs.insert_datum(resource=resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        self._closed = True

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class JPGWriter(PILWriter):
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """
    SPEC = 'jpg'
    def __init__(self, fs, root=_ROOTDIR):
        super(JPGWriter, self).__init__(fs, root=_ROOTDIR)
        self.ext = 'jpg'

class PNGWriter(PILWriter):
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """
    SPEC = 'png'
    def __init__(self, fs, root=_ROOTDIR):
        super(JPGWriter, self).__init__(fs, root=_ROOTDIR)
        self.ext = 'png'


# TODO : Add a BlankWriter, basically takes a filename but outputs nothing.
#   It also writes in the correct file handler (which should be an input)

writers_dict = {'npy' : NpyWriter, 'jpg' : JPGWriter, 'png' : PNGWriter}
