# these read the configuration file and setup extra configuration of parameters
import yaml
import os.path
import numpy as np
import numbers


# reads yaml file from user directory
filename = os.path.expanduser("~/.config/scistreams/scistreams.yml")
try:
    f = open(filename)
    config = yaml.load(f)
except FileNotFoundError:
    config = dict()


# make it an empty dict if not there
if 'directories' not in config:
    config['directories'] = dict()
directories = config['directories']

# read the mask conf file
# ignore the mask yml now, just use mask_dir
filename_masks = os.path.expanduser("~/.config/scistreams/masks.yml")
try:
    f = open(filename_masks)
    mask_config = yaml.load(f)
    for key in mask_config.keys():
        for key2 in mask_config[key].keys():
            fname = mask_config[key][key2]['filename']
            # prepend the mask directory to filename then save
            newfname = directories['masks'] + "/" + key + "/" + fname
            mask_config[key][key2]['filename'] = newfname
        # now populate the master mask databases for each mask
except FileNotFoundError:
    mask_config = dict()
# populate the masks

# TODO :clean this up, make more scalable
# should be a matter of replacing mask reading with a file handler
# and populating a database of masks with some other form of masks
master_masks = dict()
if 'masks' in directories:
    maskdir = directories.get('masks', None)
    # print("Reading masks from : {}".format(maskdir))

    # each dir is a key
    keys = os.listdir(maskdir)
    # only directories
    keys = [key for key in keys if os.path.isdir(maskdir + "/" + key)]
    # print("directories : {}".format(keys))
    for key in keys:
        master_masks[key] = list()
        # now populate each mask
        mask_subdir = maskdir + "/" + key
        # TODO : replace with file handlers
        filenames = os.listdir(mask_subdir)
        # print('reading from subdir {}'.format(mask_subdir))
        filenames = [filename for filename in filenames
                     if os.path.isfile(mask_subdir + "/" + filename)]
        # print("filenames : {}".format(filenames))
        for filename in filenames:
            # look for npz files
            fname = mask_subdir + '/' + filename
            print('loading {}'.format(fname))
            try:
                res = np.load(fname)
                print('reading items')
                newdict = dict()
                for subkey, val in res.items():
                    # don't read the actual masks
                    if 'mask' not in subkey:
                        newdict[subkey] = res[subkey]
                newdict['filename'] = fname

                master_masks[key].append(newdict)
            except Exception:
                print("Error, could not load filename {}".format(fname))


detector_names = dict(pilatus300='saxs', psccd='waxs', pilatus2M='saxs')


# no databases present, only add if config file gives some
default_databases = {}


_DEFAULTS = {
    'delayed': True,
    'debug': False,
    'required_attributes': dict(main=dict()),
    'default_timeout': None,
    'storagedir': os.path.expanduser("~/storage"),
    'maskdir': os.path.expanduser("~/storage/masks"),
    'resultsroot': os.path.expanduser("/GPFS/pipeline"),
    'filestoreroot': os.path.expanduser("~/sqlite/filestore"),
    'delayed': True,
    'server': None,
    'databases': default_databases,
    # tensorflow storage stuff
    'TFLAGS': {'out_dir': '/GPFS/pipeline/ml-tmp',
               'num_batches': 16}
}


# get from directories file
storagedir = directories.get('storage', _DEFAULTS['storagedir'])
maskdir = directories.get('masks', _DEFAULTS['maskdir'])

default_timeout = config.get('default_timeout', _DEFAULTS['default_timeout'])
delayed = config.get('delayed', _DEFAULTS['delayed'])
resultsroot = config.get('resultsroot', _DEFAULTS['resultsroot'])
required_attributes = config.get('required_attributes',
                                 _DEFAULTS['required_attributes'])
debug = config.get('debug', _DEFAULTS['debug'])

# TODO : need way of dynamically doing this
modules = config.get('modules', {})
tensorflow = modules.get('tensorflow', {})


# TODO : formalize this with some global existing python structure?
# currently used for metadata validation
typesdict = {'int': int,
             'float': float,
             'str': str,
             'number': numbers.Number,
             }


def validate_md(md, name="main", validate_dict=None):
    ''' This just validates metadata
        Cycles through internal dictionary of key : typestr
        pairs where typestr is either 'int', 'float', 'str' or something else
        etc.

        name : the name from validation dictionary to pull from
        validate_dict : an external validation dictionary to use
    '''
    # first check kwarg, then define if not
    if validate_dict is None:
        validate_dict = required_attributes.get(name, {})
    for key, val in validate_dict.items():
        if key not in md:
            errormsg = "Error, key {} not in metadata".format(key)
            raise KeyError(errormsg)
        valtype = typesdict.get(val, None)
        if valtype is None:
            errormsg = "Error, type not understood for validation"
            errormsg += "\n Please check your validation definitions for "
            errormsg += "the class {}".format(name)
            errormsg += "\nThe value type is '{}'".format(val)
            errormsg += "\nPlease ensure it looks correct."
            raise ValueError(errormsg)

        if not isinstance(md[key], typesdict[val]):
            errormsg = "Error, key {}".format(key)
            errormsg += " is not an instance of {}".format(key, val)
            raise TypeError(errormsg)

    return True


TFLAGS_tmp = dict()
TFLAGS_tmpin = config.get("TFLAGS", _DEFAULTS['TFLAGS'])
for key in _DEFAULTS['TFLAGS']:
    TFLAGS_tmp[key] = TFLAGS_tmpin.get(key, _DEFAULTS['TFLAGS'][key])


class TFLAGS:
    pass


for key, val in TFLAGS_tmp.items():
    setattr(TFLAGS, key, val)

if isinstance(resultsroot, list):
    resultsrootmap = resultsroot
    resultsroot = None
else:
    resultsrootmap = None


server = config.get('server', _DEFAULTS['server'])
databases = config.get('databases', _DEFAULTS['databases'])


if delayed:
    from dask import delayed
else:
    def delayed(pure=None, pure_default=None):
        def dec(f):
            def fnew(*args, **kwargs):
                return f(*args, **kwargs)
            return fnew
        return dec
