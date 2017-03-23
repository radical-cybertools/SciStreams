# this is like a dict but I can identify it
# also, hashes well in dask (for nested dicts/SciResults too)


# Assumed paramters:
# _name : the protocol name
# _output_names
# _run_stats
# _files
# _SciResult : to identify it
class SciResult(dict):
    ''' Something to distinguish a dictionary from
        but in essence, it's just a dictionary.'''

    def __init__(self, **kwargs):
        super(SciResult, self).__init__(**kwargs)
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

    def get(self):
        ''' return the results as would be expected from the function
        itself.'''
        args = list()
        for output_name in self['_output_names']:
            args.append(self[output_name])
        return args

# transform databroker header to a SciResult
def header2SciResult(header,events=None):
    ''' Convert databroker header to a SciResult.'''
    pass
