# TODO : change this to some global analysis directory
# and make sure it works in a GLOBAL filesystem for distributed environment
_ROOTDIR = "/home/lhermitte/sqlite/fs-tmp-data"
import os
import uuid
import numpy as np

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

# TODO : Add a BlankWriter, basically takes a filename but outputs nothing.
#   It also writes in the correct file handler (which should be an input)

writers_dict = {'npy' : NpyWriter}
