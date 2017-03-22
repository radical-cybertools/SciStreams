from SciAnalysis.contrib.decorators import simple_func2classdecorator
from dask import delayed
from SciAnalysis.SciResult import SciResult
from SciAnalysis.decorators import parse_sciresults
from copy import copy
import time

def run_default(inner_function):
    '''Standard book-keeping required for the 'run' method of any protocol.'''
    def _run_default(*args, **kwargs):
        if '_run_stats' not in kwargs:
            kwargs['_run_stats'] = dict()

        run_stats = kwargs['_run_stats']
        run_stats['start_timestamp'] = time.time()

        # results should be a SciResult
        results = inner_function(*args, **kwargs)

        run_stats['end_timestamp'] = time.time()

        results['_run_stats'] = run_stats

        return results

    return _run_default

# the Protocol decorator
def Protocol(name="", output_names=list(), keymap = dict(), accepted_args=list(),
        defaults={}):
    @simple_func2classdecorator
    def decorator(f):
        class MyClass:
            _accepted_args = accepted_args
            _keymap = keymap
            _output_names = output_names
            _name = name
    
            def __init__(self, **kwargs):
                self.kwargs = defaults
                self.kwargs.update(**kwargs)
        
            def run(self, **kwargs):
                # first update kwargs with incoming
                new_kwargs = self.kwargs.copy()
                new_kwargs.update(**kwargs)
                new_kwargs['_accepted_args'] = self._accepted_args
                return self.run_explicit(_name=self._name, **new_kwargs)
        
            @delayed(pure=True)
            @run_default
            @parse_sciresults(keymap, output_names)
            # need **kwargs to allow extra args to be passed
            def run_explicit(*args, **kwargs):
                # next parse out unaccepted arguments
                # (but allow them to pass here first)
                # only pass accepted args
                new_kwargs = dict()
                for key in kwargs['_accepted_args']:
                    if key in kwargs:
                        new_kwargs[key] = kwargs[key]
                kwargs.pop('_name')
                return f(*args, **kwargs)
    
        return MyClass
    return decorator

# Adding functions to an existing class
# TODO : make this work. For now, doesn't
#   Current issues : new method is not bound, it is unbound
def NewClassMethod(f):
    def decorator(cls):
        class newclass(cls):
            def __init__(self, *args, **kwargs):
                super(newclass, self).__init__(*args, **kwargs)
                self.__dict__[f.__name__] = f
        return newclass
    return decorator
