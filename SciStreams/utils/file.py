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


def check_and_get(md, name, default="unnamed", strict=False):
    if name not in md:
        if strict:
            raise ValueError("Error cannot find {}".format(name) +
                             " in attributes. Not saving.")
        else:
            print("Warning, could not find {}".format(name) +
                  " in attributes. Replacing with {}".format(default))
    val = str(md.get(name, default))
    return val


def _make_fname_from_attrs(attrs, filetype="xml", strict=False,
                           stream_name=None):
    ''' make filename from attributes.
        This will likely be copied among a few interfaces.
        suffix : the suffix
        strict : bool, optional
            if True, raise ValueErrors upon missing entries.
            False, try best to reconcile issue
    '''

    experiment_alias_directory = \
        check_and_get(attrs, "experiment_alias_directory", strict=strict)
    # remove the trailing slash
    rootdir = experiment_alias_directory.strip("/")

    if _ROOTMAP is not None:
        rootdir = rootdir.replace(_ROOTMAP[0], _ROOTMAP[1])
    elif _ROOTDIR is not None:
        rootdir = _ROOTDIR

    detector_name = check_and_get(attrs, 'detector_name', strict=strict,
                                  default="unnamed")

    detector_savedir = config.detector_names.get(detector_name, detector_name)

    sample_savename = check_and_get(attrs, 'sample_savename', strict=strict,
                                    default="unnamed")
    sample_savename = _cleanup_str(sample_savename)

    if stream_name is None:
        stream_name = check_and_get(attrs, 'stream_name', strict=strict,
                                    default='unnamed_stream')
        stream_name = _cleanup_str(stream_name)

    scan_id = check_and_get(attrs, 'scan_id', strict=strict, default='scan_id')

    seq_num = check_and_get(attrs, 'seq_num', strict=strict, default=0)
    seq_num = str(seq_num)
    print("Got sequence number : {}\n\n".format(seq_num))

    outdir = rootdir + "/" + detector_savedir + "/" + stream_name \
        + "/" + filetype
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id + "_" + seq_num + "." + filetype

    return outfile
