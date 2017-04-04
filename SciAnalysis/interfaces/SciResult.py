'''
    This code is where most of the SciResult processing in the application
    layer should reside. Conversions from other interfaces to SciResult are
    found in corresponding interface folders.
'''
from uuid import uuid4
from collections import Counter

_MAX_STR_LEN = 72  # Maximum length for string representations of objects


class SciResult(dict):
    ''' The central result object processed by the analysis pipeline.

        It's basically something to distinguish a dictionary from but in
        essence, it's just a dictionary.

        Components
        ----------

        uid : the unique identifier of the SciResult. This can be trusted by
            other methods to be unique

        outputs : dict
            the outputs

        output_names : list
            ordered list of output keys

        attributes : dict
            the attributes (metadata) of the SciResult
            As functions gradually process SciResults and return new ones, this
            grows into a tree of attributes. The tree itself is not strictly
            defined and could be anything (i.e. you may not necessarily be able
            to reconstruct a task graph from this)

        global_attributes : list
            the list of global attributes of the SciResult When a function
            computes on a SciResult, its returned SciResult will inherit these
            attributes (taken from the attributes component)

        run_stats : dict
            these are metadata that describes the statistics of the run they're
            generally useful to keep separate from the rest of the metadata

        Methods
        -------
        __call__() :
            The central method for the SciResult. The API is as follows:

            scires() : return a copy of itself

            scires({'q0' : 'q1'}) : return another SciResult only the entries
                selected.  Re-map the keys according to the values of this
                dictionary

            scires(['q0', 'q1']) : return a sciresult with only the entries
            mentioned selected

        printrunstats : get the timing of a run that produced this SciResult

        select :
            Select entries.

            scires.select({'q0' : 'q1'})
                select 'q0' and rename key to 'q1'

            scires.select(['q0', 'a0'])
                select the entries 'q0' and 'a0'

            Notes
            -----
                Difference between this and rename is that this only includes
                named parameters whereas rename includes all parameters.

        Notes
        -----
            This should also be serializable.
    '''
    def __init__(self, *args, **kwargs):
        super(SciResult, self).__init__()
        if len(args) == 1 and "_SciResult" in args[0]:
            self.update(dict(args[0]))
            # uid needs to be updated still
            self['uid'] = str(uuid4())
        else:
            # the outputs and output names
            self['outputs'] = dict()  # outputs
            self['output_names'] = list()  # ordering of names of outputs

            # if there are args and kwargs, they get saved to output
            for i, arg in enumerate(args):
                key = "_arg{}".format(i)
                self.addoutput(key, arg)
            for key, val in kwargs.items():
                self.addoutput(key, val)

            # the metadata. This should contain a few defaults
            self['attributes'] = dict()  # metadata

            self['global_attributes'] = list()

            # run-specific stuff
            self['run_stats'] = dict()
            self['uid'] = str(uuid4())

            # identifier
            self['_SciResult'] = 'SciResult-version1'

    def __call__(self, *args, **kwargs):
        '''
            The central method for the SciResult. The API is as follows:

            scires() : return a copy of itself

            scires({'q0' : 'q1'}) : return another SciResult only the entries
            selected.  Re-map the keys according to the values of this
            dictionary

            scires(['q0', 'q1']) : return a sciresult with only the entries
            mentioned selected
        '''
        if len(args) == 0:
            if len(kwargs) == 0:
                return SciResult(self)
            else:
                # this is a dict, so select and remap
                return self.select(kwargs)
        elif len(args) == 1:
            arg = args[0]
            if len(kwargs) > 0:
                errorstr = "Use of arguments and keyword arguments not "
                errorstr += "supported"
                raise ValueError(errorstr)
            if isinstance(arg, dict) or isinstance(arg, list) or isinstance(arg, str):
                return self.select(arg)
            else:
                raise ValueError("Did not understand input")
        else:
            raise ValueError("More than 1 args is not supported")
        # do remapping at end since a few cases require it

    def get(self):
        ''' Returns what a function that created this SciResult would normally return.
        '''
        if self.num_outputs() == 1:
            return self['outputs'][self['output_names'][0]]
        else:
            res = list()
            for name in self['output_names']:
                res.append(self['outputs'][name])

            return res

    def printrunstats(self):
        ''' Print some useful statistics of this SciResult.'''

        print("SciResult information:")
        if 'run_stats' not in self:
            print("Sorry, no run stats saved, cannot print")
            return
        runstats = self['run_stats']
        if 'start_timestamp' not in runstats or\
                'end_timestamp' not in runstats:
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

    def select(self, rename_dict):
        '''
            Select entries.

            scires.select({'q0' : 'q1'})
                select 'q0' and rename key to 'q1'

            scires.select(['q0', 'a0'])
                select the entries 'q0' and 'a0'

            Notes
            -----
                Difference between this and rename is that this only includes
                named parameters whereas rename includes all parameters.
        '''
        if isinstance(rename_dict, str):
            rename_dict = [rename_dict]
        # make a copy
        scires = SciResult(self)
        # select in place for some scires
        self._select(scires, rename_dict)
        return scires

    def _select(self, scires, rename_dict):
        if isinstance(rename_dict, dict):
            new_output_names = list(rename_dict.values())
            new_outputs = dict()
            for key, newkey in rename_dict.items():
                new_outputs[newkey] = scires['outputs'][key]
        elif isinstance(rename_dict, list):
            new_output_names = rename_dict
            new_outputs = dict()
            for key in rename_dict:
                new_outputs[key] = scires['outputs'][key]
        else:
            raise ValueError("Input must be a list or dict")
        scires['output_names'] = new_output_names
        scires['outputs'] = new_outputs

    def rename(self, rename_dict):
        ''' Rename the outputs.
            Creates a new dictionary.

            Parameters
            ----------
            rename_dict : dict
                A dictionary with the key mapping
                The dict should be of form:
                    {'a' : 'b', 'c' : ''}
                where a blank string signifies removing an entry from the
                SciResult

            Returns
            -------
            scires : SciResult
                A new SciResult instance with the keys remapped.
        '''
        # it's an ordered dictionary
        scires = SciResult(self)
        self._rename(scires, rename_dict)
        return scires

    def _rename(self, scires, rename_dict):
        if not isinstance(rename_dict, dict):
            raise ValueError("Error, must be a dictionary to rename")
        # create mapping for all elements
        rename_dict_tmp = rename_dict
        rename_dict = dict({key: key for key in scires['outputs'].keys()})
        # update the ones that are requested to be remapped
        rename_dict.update(rename_dict_tmp)

        # first check to see all keys exist in output
        for key in rename_dict.keys():
            if key not in scires['outputs'].keys():
                errorstr = "Error mapping a non existent key {}".format(key)
                raise ValueError(errorstr)

        # now check for conflicts
        cts = list(Counter(rename_dict.values()).values())
        for ct in cts:
            if ct > 1:
                raise ValueError("Error, re-mapping collision. Aborting...")

        # now do the re-mapping
        for key, newkey in rename_dict.items():
            if key != newkey:
                if len(newkey) > 0:
                    scires['outputs'][newkey] = scires[key]
                del scires['outputs'][key]

        new_names = list()
        for output_name in self['output_names']:
            new_names.append(rename_dict[output_name])

        scires['output_names'] = new_names


'''
    This decorator parses SciResult objects, indexes properly takes a keymap
    for args this unravels into arguments if necessary.
'''
# TODO :rewriting sciresult again


def parse_sciresults(protocol_name, attributes={}):
    # from input_map, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            # Initialize new SciResult
            scires = SciResult()
            scires['attributes'] = dict(attributes)

            saved_globals = list()
            # grab attributes if entries were SciResults
            # if a global attribute, grab this
            for i, entry in enumerate(args):
                # checks if it's a SciResult
                key = "_arg{}".format(i)
                if isinstance(entry, dict) and '_SciResult' in entry:
                    args[i] = entry.get()
                    _add_attribute(key, entry, scires, saved_globals)
                else:
                    scires['attributes'][key] = repr(entry)[:_MAX_STR_LEN]

            for key, entry in kwargs.items():
                # checks if it's a SciResult
                if isinstance(entry, dict) and '_SciResult' in entry:
                    kwargs[key] = entry.get()
                    _add_attribute(key, entry, scires, saved_globals)
                else:
                    scires['attributes'][key] = repr(entry)

            if '_name' not in kwargs:
                scires['attributes']['function_name'] = 'N/A'
            else:
                scires['attributes']['function_name'] = kwargs['_name']

            # Run function, if it's a tuple, splay the outputs
            result = f(*args, **kwargs)

            # Save outputs to SciResult
            if isinstance(result, dict):
                # NOTE : Order is not well defined here
                # TODO : mark in documentation that order is not well defined
                for key, val in result.items():
                    scires.addoutput(key, val)
            elif not isinstance(result, tuple):
                # add place holder names
                scires.addoutput("_unnamed_0", result)
            else:
                for i, res in enumerate(result):
                    scires.addoutput("_unnamed_{}".format(i), res)

            return scires
        return _f
    return decorator

def _add_attribute(key, scires_from, scires_to, saved_globals):
    '''
        Convenience function to add an attribute and keep track of globals.
        Mainly here because it's used twice in parse_sciresults
    '''
    scires_to['attributes'][key] = scires_from['attributes']
    for glob_attr in scires_from['global_attributes']:
        if glob_attr not in saved_globals:
            saved_globals.append(glob_attr)
            scires_to['attributes'][glob_attr] = \
                scires_from['attributes'][glob_attr]
        else:
            if not isinstance(scires_to['attributes'][glob_attr], list):
                scires_to['attributes'][glob_attr] = \
                        list(scires_to['attributes'][glob_attr])
            scires_to['attributes'][glob_attr].append(scires_from['attributes'][glob_attr])
