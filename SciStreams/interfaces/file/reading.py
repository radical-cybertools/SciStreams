from .core import jpegloader, pngloader, hdf5loader, npyloader
from .core import jpegwriter, pngwriter

from ...interfaces.StreamDoc import StreamDoc

# Simple cloass to normalize way files are read
# Add to _FORMATSPECS and _REQUIREDKEYS to handle more files
# need a 'get' and 'identify' set of routines to make compatible with
# SciStreams


class FileDesc(dict):
    ''' Some normalized way of reading files from filenames.'''
    # case insensitive
    _FORMATSPECS = {
            '.jpg': 'jpg',
            '.h5': 'hdf5',
            '.hd5': 'hdf5',
            '.png': 'png',
            '.npz': 'npy',
            '.npy': 'npy',
    }
    # required keys per format specifier
    _KEYINFO = {
            'jpg': {'required': ['filename'],
                    'loader': jpegloader,
                    'writer': jpegwriter,
                    },
            'png': {'required': ['filename'],
                    'loader': pngloader,
                    'writer': pngwriter,
                    },
            'hdf5': {'required': ['filename', 'hdf5key'],
                     'loader': hdf5loader},
            'npy': {'required': ['filename'],
                    'loader': npyloader},
    }

    def __init__(self, filename, format=None, **kwargs):
        super(FileDesc, self).__init__()
        if format is None:
            self['format'] = self.guess_format(filename)
        self['filename'] = filename
        # now add extra kwargs
        for key, val in kwargs:
            self[key] = val

    def load(self):
        format = self['format']
        loader = self._KEYINFO[format]['loader']
        kwargs = dict()
        for key in self._KEYINFO[format]['required']:
            kwargs[key] = self[key]
        res = loader(**kwargs)
        self['_data'] = res

    def write(self, data):
        format = self['format']
        writer = self._KEYINFO[format]['writer']
        kwargs = dict()
        for key in self._KEYINFO[format]['required']:
            kwargs[key] = self[key]
        kwargs['data'] = data
        writer(**kwargs)
        self['_data'] = data

    def get(self):
        ''' Get returns a SciResult.

        '''
        # TODO : fix
        scires = StreamDoc()
        if '_data' not in self:
            self.load()
        scires['output_names'] = ['data']
        scires['outputs']['data'] = self['_data']
        scires['attributes'] = self.identify()
        return scires

    def get_raw(self):
        ''' get_raw returns raw result (if needed).
            This is discouraged and recommended only for debugging.
            We should separate data from processing through the use
            of SciResult.
        '''
        if '_data' not in self:
            self.load()
        return self['_data']

    def identify(self):
        res = dict()
        format = self['format']
        attrkeys = self._KEYINFO[format]['required']
        for key in attrkeys:
            res[key] = self[key]

        return res

    def guess_format(self, filename):
        if not isinstance(filename, str):
            raise ValueError("Error, filename is not a string")
        # make case insensitive
        filename = filename.lower()

        format = None
        for key, val in self._FORMATSPECS.items():
            if filename.endswith(key.lower()):
                format = val
        if format is None:
            errorstr = "Error, extension of {} is not one of supported types".\
                format(filename)
            errorstr = errorstr + "\n Supported are : {}".\
                format(list(self._FORMATSPECS.keys()))
            raise ValueError(errorstr)

        return format

    def verify(self):
        ''' Verify that the loaded format is accepted.'''
        format = self['format']
        req_keys = self._KEYINFO[format]['required']
        for key in req_keys:
            if key not in self:
                errorstr = "Sorry, {} specifier requires a {} parameter".\
                    format(self['format'], key)
                raise ValueError(errorstr)
