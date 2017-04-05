from dask import delayed
from copy import copy
import time

from SciAnalysis.interfaces.SciResult import SciResult, parse_sciresults
from SciAnalysis.interfaces.databroker import dbtools

def run_default(inner_function):
    '''Standard book-keeping required for the 'run' method of any protocol.
        Note : only works on a SciResult returned result.
        Make sure function outputs SciResult (usually by just decorating it)
    '''
    def _run_default(*args, **kwargs):
        #if '_run_stats' not in kwargs or not isinstance(kwargs['_run_stats'], dict):
            #kwargs['_run_stats'] = dict()

        run_stats = dict()
        #run_stats = kwargs['_run_stats']
        run_stats['start_timestamp'] = time.time()
        # results should be a SciResult
        results = inner_function(*args, **kwargs)
        run_stats['end_timestamp'] = time.time()
        run_stats['runtime'] = run_stats['end_timestamp'] - run_stats['start_timestamp']

        results['run_stats'] = run_stats

        return results

    return _run_default

class Protocol:
    '''
        The base Protocol class. You can use it as a template or inherit it (the
        latter is strongly encouraged).

        The four decorators shown need to be added, depending on what you want
        to do.
        @delayed : delays computations for distributed computing
        @store_results : stores the results into databroker
        @run_default : adds timing results to the results
        @parse_sciresults : parses inputs/outputs and transforms them to a normalized
            SciResult dictionary.

    '''
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, **kwargs):
        new_kwargs = dict()
        new_kwargs.update(self.kwargs.copy())
        new_kwargs.update(kwargs)
        return self.run(**new_kwargs)

    # This is just an example
    @delayed(pure=True)
    @dbtools.store_results('cms:analysis')
    @run_default
    @parse_sciresults("XS:calibration")
    def run(calibration={}, **kwargs):
        raise NotImplementedError("You should inherit this class and add run function.")
