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

def _make_fname_from_attrs(attrs):
    ''' make filename from attributes.
        This will likely be copied among a few interfaces.
    '''
    if 'experiment_cycle' not in attrs:
        raise ValueError("Error cannot find experiment_cycle in attributes")
    else:
        experiment_cycle = _cleanup_str(attrs['experiment_cycle'])

    if 'detector_name' not in attrs:
        raise ValueError("Error cannot find detector_name in attributes")
    else:
        detector_name = _cleanup_str(attrs['detector_name'])

    if 'experiment_group' not in attrs:
        raise ValueError("Error cannot find experiment_group in attrbutess")
    else:
        experiment_group = _cleanup_str(attrs['experiment_group'])

    if 'sample_savename' not in attrs:
        raise ValueError("Error cannot find sample_savename in attributes")
    else:
        sample_savename = _cleanup_str(attrs['sample_savename'])

    if 'stream_name' not in attrs:
        #raise ValueError("Error cannot find stream_name in attributes")
        stream_name = 'unnamed_analysis'
    else:
        stream_name = _cleanup_str(attrs['stream_name'])

    if 'scan_id' not in attrs:
        raise ValueError("Error cannot find scan_id in attributes")
    else:
        scan_id = _cleanup_str(str(attrs['scan_id']))

    outdir = _ROOTDIR + "/" + experiment_cycle + "/" + experiment_group + "/" + detector_name + "/" + stream_name
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id

    return outfile


def store_results_file(results, writers={}):
    ''' Store the results to a numpy file.
        This saves to numpy format by default.
        May raise an error if it doesn't understand data.

        For images, you'll need to use a plotting/image interface (not implemented yet).
    '''
    if 'kwargs' not in results:
        raise ValueError("kwargs not in the sciresults. (Is this a valid SciResult object?)")
    results_dict = results['kwargs']
    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")
    attrs = results['attributes']


    # prepare directory
    outfile = _make_fname_from_attrs(attrs)

    if not isinstance(writers, list):
        writers = [writers]
    for writer_entry in writers:
        # go through each writing instruction
        writer_key = writer_entry['writer']

        writer = writers_dict[writer_key]

        keys = writer_entry['keys']
        # group the data together
        data = OrderedDict()
        if not isinstance(keys, list):
            keys = [keys]
        for key in keys:
            data.update({key : results_dict[key]})
        writer(filename=outfile, data=data)
