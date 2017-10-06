import os

from .. import config
_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap


def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs(directory)


def access_dir(base, extra=''):
    '''Returns a string which is the desired output directory.
    Creates the directory if it doesn't exist.'''

    output_dir = os.path.join(base, extra)
    make_dir(output_dir)

    return output_dir


def get_filebase(name):
    basename = os.path.basename(name)
    basename, ext = os.path.splitext(basename)
    return basename


def _cleanup_str(string):
    string = string.replace(" ", "_")
    string = string.replace("/", "_")
    string = string.replace("(", "_")
    string = string.replace(")", "_")
    string = string.replace(":", "_")
    return string


def _make_fname_from_attrs(attrs, filetype="xml"):
    ''' make filename from attributes.
        This will likely be copied among a few interfaces.
        suffix : the suffix
    '''
    if 'experiment_alias_directory' not in attrs:
        raise ValueError("Error cannot find experiment_alias_directory" +
                         " in attributes. Not saving.")

    # remove the trailing slash
    rootdir = attrs['experiment_alias_directory'].strip("/")

    if _ROOTMAP is not None:
        rootdir = rootdir.replace(_ROOTMAP[0], _ROOTMAP[1])
    elif _ROOTDIR is not None:
        rootdir = _ROOTDIR

    if 'detector_name' not in attrs:
        raise ValueError("Error cannot find detector_name in attributes")
    else:
        detname = _cleanup_str(attrs['detector_name'])
        # get name from lookup table first
        detector_name = config.detector_names.get(detname, detname)

    if 'sample_savename' not in attrs:
        raise ValueError("Error cannot find sample_savename in attributes")
    else:
        sample_savename = _cleanup_str(attrs['sample_savename'])

    if 'stream_name' not in attrs:
        # raise ValueError("Error cannot find stream_name in attributes")
        stream_name = 'unnamed_analysis'
    else:
        stream_name = _cleanup_str(attrs['stream_name'])

    if 'scan_id' not in attrs:
        raise ValueError("Error cannot find scan_id in attributes")
    else:
        scan_id = _cleanup_str(str(attrs['scan_id']))

    outdir = rootdir + "/" + detector_name + "/" + stream_name + "/" + filetype
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id + "." + filetype

    return outfile
