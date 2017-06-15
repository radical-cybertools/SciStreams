from dask import delayed
from copy import copy
import time

from SciAnalysis.interfaces.SciResult import SciResult, parse_sciresults
from SciAnalysis.interfaces.databroker import databroker as source_databroker

from functools import wraps

def run_stats(inner_function):
    ''' Add statistics to the sciresult from a run method.
    '''
    def _run_stats(*args, **kwargs):
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

    return _run_stats

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
    def run(calibration={}, **kwargs):
        raise NotImplementedError("You should inherit this class and add run function.")

# a decorator that does nothing
def noop_decorator(f):
    @wraps(f)
    def f_new(*args, **kwargs):
        return f(*args, **kwargs)
    return f_new

def noop_decorator_constructor(*args, **kwargs):
    return noop_decorator

from SciAnalysis.interfaces.xml import xml as source_xml
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.SciResult import parse_sciresults

def run_default(protocol_name, xml=True, file=True, databroker=True, delay=True,
        xml_options=None, file_options=None, databroker_options=None):
    '''Standard book-keeping required for the 'run' method of any protocol.
        Note : only works on a SciResult returned result.
        Make sure function outputs SciResult (usually by just decorating it)

        xml : if True, save to xml
            This will look for certain metadata attributes to save into the appropriate directory

        savefiles : if True, save the data to files
            This will look for certain metadata attributes to save into the appropriate directory

        databroker : if has a `name`h entry, will look for that database name
            if also has an optional `writers` entry, will look for these writers
            
        delay : if True, delay the object using dask
            Note : dask delayed has options set globally. For example,
            different caches can be used. It can also be distributed or
            non-distributed.
    '''
    if xml_options is None:
        xml_options = dict(outputs=[])

    if databroker_options is None:
        databroker_options = dict(name="cms:analysis", writers={})

    if file_options is None:
        file_options = {}

    delay_options = dict(pure=True)

    if not delay:
        delay_decorator = noop_decorator
    else:
        delay_decorator = delayed(**delay_options)

    if not xml:
        xml_decorator = noop_decorator
    else:
        xml_decorator = source_xml.store_results(**xml_options)

    if not file:
        file_decorator = noop_decorator
    else:
        file_decorator = source_file.store_results(**file_options)

    if not databroker:
        databroker_decorator = noop_decorator
    else:
        if 'name' not in databroker_options:
            raise ValueError("No name set in databroker options, cannot save using databroker")
        name = databroker_options['name']
        if 'writers' not in databroker_options:
            writers = {}
        else:
            writers = databroker_options['writers']
        databroker_decorator = source_databroker.store_results(name, writers)

    def run_decorator(f):
        @delay_decorator
        @wraps(f)
        @databroker_decorator
        @xml_decorator
        @file_decorator
        @run_stats
        @parse_sciresults(protocol_name)
        def f_new(*args, **kwargs):
            return f(*args, **kwargs)
        return f_new

    return run_decorator
