'''
 example:

        from SciAnalysis.interfaces.file.reading import FileDesc
        from SciAnalysis.datatypes.Mask import Mask
        mask = Mask(FileDesc("../foo.png"))

'''

class Mask(dict):
    ''' 
        Mask is just a dict with extra details

        Takes a fileobj. This fileobj can be anythin (not necessary a file)
            Only requirement is that it has a `get` and `identify` routine.
            fileobj.`get` should return raw data
            fileobj.`identify` should return a dictionary of metadata

    Typical usage:
        mask = Mask(filename)
        
    To get:
        mask_data = mask.get()
    
    '''
    def __init__(self, fileobj=None, threshold=None, dtype=None, invert=None, **kwargs):
        self.load(fileobj)
        self['_data_attrs'] = fileobj.identify()
        # create attributes for mask
        self['_mask_attrs'] = dict()
        self.set_threshold(threshold=threshold)
        self.set_dtype(dtype=dtype)

    def load(self, fileobj=None):
        self['_data'] = fileobj.get()

    def get(self):
        return self['_data']

    def export(self):
        return dict(self)


    def set_threshold(self, threshold=None):
        if threshold is not None:
            self['_data'] *= self['_data'] > threshold
        self._set_mask_attr('threshold', threshold)

    def set_dtype(self, dtype=None):
        if dtype is not None:
            self['_data'] = self['_data'].astype(dtype)
        else:
            dtype = self['_data'].dtype
        self._set_mask_attr('dtype', dtype)

    def _set_mask_attr(self, key, val):
        self['_mask_attrs'][key] = val

    def __repr__(self):
        return "Mask Object : \n" + str(self["_data"])
