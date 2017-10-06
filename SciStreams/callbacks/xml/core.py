# xml callbacks
from collections import OrderedDict
from ...interfaces.file.core import writers_dict
from ... import config
from ...interfaces.file.reading import FileDesc  # noqa
from ...utils.file import _make_fname_from_attrs

_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap


def store_results_file(results, writers={}):
    ''' Store the results to a numpy file.
        This saves to numpy format by default.
        May raise an error if it doesn't understand data.

        For images, you'll need to use a plotting/image interface (not
        implemented yet).
    '''
    if 'kwargs' not in results:
        raise ValueError("kwargs not in the sciresults. " +
                         "(Is this a valid SciResult object?)")
    results_dict = results['kwargs']
    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. " +
                         "(Is this a valid SciResult object?)")
    attrs = results['attributes']

    # prepare directory
    outfile = _make_fname_from_attrs(attrs)
    print("writing to {}".format(outfile))

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
            data.update({key: results_dict[key]})
        writer(filename=outfile, data=data)
