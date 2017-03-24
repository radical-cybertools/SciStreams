'''
    A SciResult is like a dict but I can identify it
        Also, hashes well in dask (for nested dicts/SciResults too)

 Assumed paramters:
    _name : the protocol name
    _output_names
    _run_stats
    _files
    _SciResult : to identify it

All conversions to SciResult are in the respective interface libraries.


TODO : 
    1. Allow nested keymaps in parse_sciresults?
    2. Explain this also cleans all other parameters not specified by output
'''


class SciResult(dict):
    ''' Something to distinguish a dictionary from but in essence, it's just a
    dictionary.
    
        It contains convenience methods:
            printrunstats : get the timing of a run that produced this SciResult
            get : get the actual raw data that would have been returned by this
                SciResult
    '''

    def __init__(self, *args, **kwargs):

        if len(args) == 1:
            kwargs.update(dict(args))
        super(SciResult, self).__init__(**kwargs)
        self['_name'] = ""
        self['_output_names'] = list() # name of the data
        self['_run_stats'] = dict()
        self['_attributes'] = dict() #metadata
        self['_files'] = dict()
        # identifier, needed for Dask which transforms SciResult into a dict
        # TODO : Suggest to dask that classes should not be modified
        self['_SciResult'] = 'SciResult-version1'

    def printrunstats(self):
        if '_run_stats' not in self:
            print("Sorry, no run stats saved, cannot print")
            return 
        runstats = self['_run_stats']
        if 'start_timestamp' not in runstats or 'end_timestamp' not in runstats:
            print("Sorry, no information about timing, cannot print")
            return
        time_el = runstats['end_timestamp']-runstats['start_timestamp']
        print("Run stats")
        print("Time Elapsed : {} s".format(time_el))

    def addoutput(self, name, val):
        pass

    def get(self):
        ''' return the results as would be expected from the function
        itself.'''
        args = list()
        for output_name in self['_output_names']:
            args.append(self[output_name])
        return args

    def num_outputs(self):
        return len(self['_output_names'])

    def verify(self):
        ''' Verify that this is a valid SciResult.'''
        # TODO : write this
        pass

'''
    This decorator parses SciResult objects, indexes properly takes a keymap for
    args this unravels into arguments if necessary.
'''
def parse_sciresults(keymap, output_names):
    # from keymap, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            for i, val in enumerate(args):
                # checks if it's a SciResult
                if isinstance(val, dict) and '_SciResult' in val:
                    key = "_arg{}".format(i)
                    args[i] = val[keymap[key]]
            for key, val in kwargs.items():
                # checks if it's a SciResult
                if isinstance(val, dict) and '_SciResult' in val:
                    kwargs[key] = val[keymap[key]]
            resultdict = kwargs.copy()
            for key, val in list(resultdict.items()):
                if not key.startswith("_"):
                    resultdict.pop(key)
            result = f(*args, **kwargs)
            if len(output_names) == 1:
                resultdict.update({output_names[0] : result})
            else:
                resultdict.update({output_names[i] : res for i, res in enumerate(result)})
            # this is so databroker can know how to reproduce function result
            resultdict['_output_names'] = output_names

            return SciResult(**resultdict)
        return _f
    return decorator
