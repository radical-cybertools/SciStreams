'''
 example:

        from SciAnalysis.interfaces.file.reading import FileDesc
        from SciAnalysis.datatypes.Mask import Mask
        mask = Mask(FileDesc("../foo.png"))

'''

class Mask(dict):
    ''' 
        Mask is just a dict with extra details

        Takes a SciResult. The SciResult is assumed to contain just one output.

    Typical usage:
        scires = FileDesc("foo.png").get()
        mask = Mask(scires)
        
    To get:
        mask_data = mask.get()
    
    '''
    def __init__(self, scires=None, threshold=None, dtype=None, invert=None, **kwargs):
        self.load(scires)
        # create attributes for mask
        self['_mask_attrs'] = dict()
        self.set_threshold(threshold=threshold)
        self.set_dtype(dtype=dtype)

    def load(self, scires=None):
        if scires.num_outputs() != 1:
            raise ValueError("Sorry, this SciResult contains too many outputs")
        self['_data'] = scires.get()

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
