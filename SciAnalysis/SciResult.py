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

# This decorator parses SciResult objects, indexes properly
# takes a keymap for args
# this unravels into arguments if necessary
# TODO : Allow nested keymaps
def parse_sciresults(keymap, output_names):
    # from keymap, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            for i, val in enumerate(args):
                if isinstance(val, SciResult):
                    key = "_arg{}".format(i)
                    args[i] = val[keymap[key]]
            for key, val in kwargs.items():
                if isinstance(val, SciResult):
                    kwargs[key] = val[keymap[key]]
            result = f(*args, **kwargs)
            if len(output_names) == 1:
                result = {output_names[0] : result}
            else:
                result = {output_names[i] : res for i, res in enumerate(result)}

            return SciResult(**result)
        return _f
    return decorator
