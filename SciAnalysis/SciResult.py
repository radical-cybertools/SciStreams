# this is like a dict but I can identify it
# also, hashes well in dask (for nested dicts/SciResults too)
class SciResult(dict):
    ''' Something to distinguish a dictionary from
        but in essence, it's just a dictionary.'''

    def __init__(self, **kwargs):
        super(SciResult, self).__init__(**kwargs)
        # identifier, needed for Dask which transforms SciResult into a dict
        # TODO : Suggest to dask that classes should not be modified
        self['_SciResult'] = 'SciResult-version1'
