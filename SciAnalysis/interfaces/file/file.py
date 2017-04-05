from SciAnalysis.interfaces.file.reading import FileDesc
import os

_ROOTDIR = "/home/lhermitte/sqlite/file-tmp-data"

import numpy as np

def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs( directory )

# store results decorator

# store results decorator
def store_results(f):
    def f_new(*args, **kwargs):
        res = f(*args, **kwargs)
        store_results_file(res)
        return res
    return f_new

def store_results_file(results):
    ''' Store the results to a numpy file.
        This saves to numpy format by default.
        May raise an error if it doesn't understand data.

        For images, you'll need to use a plotting/image interface (not implemented yet).
    '''

    if 'outputs' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")
    data = results['outputs']

    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")
    attrs = results['attributes']
    
    if 'experiment_cycle' not in attrs or 'experiment_group' not in attrs or 'sample_savename' not in attrs or 'protocol_name' not in attrs:
        raise ValueError("Error cannot find experiment_cyle, experiment_group or sample_savename in results"
                "Either results object is not a sciresult or it has not been formatted properly."
                "Cannot save to XML")

    experiment_cycle = attrs['experiment_cycle']
    protocol_name = attrs['protocol_name']
    experiment_group = attrs['experiment_group']
    sample_savename = attrs['sample_savename']
    outdir = _ROOTDIR + "/" + experiment_cycle + "/" + experiment_group + "/" + protocol_name
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + ".npz"
    np.savez(outfile, **data)
