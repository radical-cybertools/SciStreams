from collections import OrderedDict
from SciAnalysis.interfaces.file.reading import FileDesc
from SciAnalysis.interfaces.file.core import writers_dict
import os

_ROOTDIR = "/home/lhermitte/sqlite/file-tmp-data"

import numpy as np

def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs( directory )

# store results decorator

# store results decorator for file
# as of now function that returns decorator takes no arguments
def store_results(**options):
    def store_results_decorator(f):
        def f_new(*args, **kwargs):
            res = f(*args, **kwargs)
            store_results_file(res, **options)
            return res
        return f_new
    return store_results_decorator

def _cleanup_str(string):
    string = string.replace(" ", "_")
    string = string.replace("/", "_")
    string = string.replace("(", "_")
    string = string.replace(")", "_")
    string = string.replace(":", "_")
    return string

def store_results_file(results, writers={}):
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
    
    if 'experiment_cycle' not in attrs:
        raise ValueError("Error cannot find experiment_cycle in attributes")
    if 'experiment_group' not in attrs:
        raise ValueError("Error cannot find experiment_group in attrbutess") 
    if 'sample_savename' not in attrs:
        raise ValueError("Error cannot find sample_savename in attributes")
    if 'protocol_name' not in attrs:
        raise ValueError("Error cannot find protocol_name in attributes")

    experiment_cycle = attrs['experiment_cycle']
    experiment_cycle = _cleanup_str(experiment_cycle)
    experiment_group = attrs['experiment_group']
    experiment_group = _cleanup_str(experiment_group)
    sample_savename = attrs['sample_savename']
    sample_savename = _cleanup_str(sample_savename)
    protocol_name = attrs['protocol_name']
    protocol_name = _cleanup_str(protocol_name)
    outdir = _ROOTDIR + "/" + experiment_cycle + "/" + experiment_group + "/" + protocol_name
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename
    if not isinstance(writers, list):
        writers = [writers]
    for writer_entry in writers:
        print("cycling through writer {}".format(writer_entry))
        # go through each writing instruction
        writer_key = writer_entry['writer']
        print("making a {} writer".format(writer_key))
        writer = writers_dict[writer_key]
        keys = writer_entry['keys']
        # group the data together
        data = OrderedDict()
        if not isinstance(keys, list):
            keys = [keys]
        for key in keys:
            data.update({key:results['outputs'][key]})
        writer(filename=outfile, data=data)
