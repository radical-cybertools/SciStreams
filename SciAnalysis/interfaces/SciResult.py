'''
    A SciResult is like a dict but I can identify it
        Also, hashes well in dask (for nested dicts/SciResults too)

 Assumed paramters passed in functions:
    _SciResult : to identify it
    _name : function name passed

All conversions to SciResult are in the respective interface libraries.

Structure of SciResult:
    - attributes : the attributes (metadata)
        NOTE : Not necessarily unique, up to user. However, for computations,
            dask does ensure uniqueness of computations by hasing arguments.
    - run_stats : statistics specific to the run instance
    - outputs : the outputs (in a dictionary)
    - output_names : a list showing the ordering of the outputs


TODO : 
    1. Allow nested keymaps in parse_sciresults?
    2. Explain this also cleans all other parameters not specified by output
'''

from collections import OrderedDict


_MAX_STR_LEN = 72

class SciResult(dict):
    ''' Something to distinguish a dictionary from but in essence, it's just a
    dictionary.
    
        It contains convenience methods:
            printrunstats : get the timing of a run that produced this SciResult
            get : get the actual raw data that would have been returned by this
                SciResult

        This should also be serializable.
    '''

    # TODO : MAJOR rewrite of SciResult right now
    def __init__(self, *args, **kwargs):
        super(SciResult, self).__init__()
        if len(args) == 1 and "_SciResult" in args[0]:
            self.update(dict(args[0]))
        else:
            # the outputs and output names
            self['outputs'] = dict() # outputs
            self['output_names'] = list() # ordering of names of outputs
    
            # if there are args and kwargs, they get saved to output
            for i, arg in enumerate(args):
                key = "_arg{}".format(i)
                self.addoutput(key, arg)
            for key, val in kwargs.items():
                self.addoutput(key, val)
    
            # the metadata. This should contain a few defaults
            self['attributes'] = dict() #metadata
            attributes = self['attributes']
    
    
            # run-specific stuff
            self['run_stats'] = dict()
    
            # identifier
            self['_SciResult'] = 'SciResult-version1'


    def printrunstats(self):
        ''' Print some useful statistics of this SciResult.'''

        print("SciResult information:")
        if 'selected_outputs' not in self or self['selected_outputs'] is None:
            print("Note outputs are currently selected. get() method won't return all outputs.")

        if 'run_stats' not in self:
            print("Sorry, no run stats saved, cannot print")
            return 
        runstats = self['run_stats']
        if 'start_timestamp' not in runstats or 'end_timestamp' not in runstats:
            print("Sorry, no information about timing, cannot print")
            return
        time_el = runstats['end_timestamp']-runstats['start_timestamp']
        print("Run stats")
        print("Time Elapsed : {} s".format(time_el))

    def addoutput(self, name, val):
        '''
            Add an output
                name : the name of the output
                val : the value of the output
        '''
        self['outputs'][name] = val
        self['output_names'].append(name)

    def get(self, name=None):
        ''' return the results as would be expected from the function
        itself.
            Note : returns object if one output, or a tuple otherwise
        '''
        if name is None:
            if 'selected_outputs' in self and self['selected_outputs'] is not None:
                name = self['selected_outputs']

        if name is not None:
            if isinstance(name, str):
                result = self['outputs'][name]
            elif isinstance(name, tuple) or isinstance(name, list):
                names = name
                result = [self['outputs'][name] for name in names]
            else:
                raise ValueError("Sorry could not understand the entry to get."
                        "Please verify this SciResult")
        else:
            print("outputs")
            print(self['outputs'])
            args = list()
            for output_name in self['output_names']:
                args.append(self['outputs'][output_name])
            result = args

        if len(result) == 1:
            result = result[0]

        return result

    def num_outputs(self):
        return len(self['output_names'])

    def verify(self):
        ''' Verify that this is a valid SciResult.'''
        # TODO : write this. Not necessary maybe?
        pass

    def valid(self, name):
        ''' verify that name is a valid entry.'''
        if name in self['output_names']:
            return True
        else:
            return False


    def select(self, outputs):
        ''' This returns an instance of scires whose outputs are chosen to be
            different from its normal output.  This does not copy the data, just
            the higher level dictionary-related data.
        '''
        # only copies the top layer
        scires = SciResult(self)
        scires['selected_outputs'] = outputs
        return scires



'''
    This decorator parses SciResult objects, indexes properly takes a keymap for
    args this unravels into arguments if necessary.
'''
def parse_sciresults(input_map, output_names, attributes={}):
    # from input_map, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            # Initialize new SciResult
            scires = SciResult()
            scires['attributes'] = dict()


            # First transform any SciResult into data, based on input_map
            # grab attributes if entries were SciResults
            for i, entry in enumerate(args):
                # checks if it's a SciResult
                key = "_arg{}".format(i)
                if isinstance(entry, dict) and '_SciResult' in entry:
                    if key in input_map and entry.valid(input_map[key]):
                        args[i] = entry.get(name=input_map[key])
                    else:
                        args[i] = entry.get()
                    scires['attributes'][key] = entry['attributes']
                else:
                    scires['attributes'][key] = repr(entry)[:_MAX_STR_LEN]

            for key, entry in kwargs.items():
                # checks if it's a SciResult
                if isinstance(entry, dict) and '_SciResult' in entry:
                    if key in input_map and entry.valid(input_map[key]):
                        kwargs[key] = entry.get(name=input_map[key])
                    else:
                        kwargs[key] = entry.get()
                    scires['attributes'][key] = entry['attributes']
                else:
                    scires['attributes'][key] = repr(entry)

            if '_name' not in kwargs:
                scires['attributes']['function_name'] = 'N/A'
            else:
                scires['attributes']['function_name'] = kwargs['_name']

            # Run function
            result = f(*args, **kwargs)

            # Save outputs to SciResult
            if len(output_names) == 1:
                scires.addoutput(output_names[0], result)
            else:
                for i, res in enumerate(result):
                    scires.addoutput(output_names[i], res)

            return scires
        return _f
    return decorator
