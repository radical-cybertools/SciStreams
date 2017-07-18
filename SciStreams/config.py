import yaml
import os.path
# reads yaml file from user directory
filename = os.path.expanduser("~/.config/scistreams/scistreams.yml")
try:
    f = open(filename)
    config = yaml.load(f)
except FileNotFoundError:
    config = dict()

detector_names = dict(pilatus300='saxs', psccd='waxs')


# no databases present, only add if config file gives some
default_databases = {}


_DEFAULTS = {
    'delayed': True,
    'storagedir': os.path.expanduser("~/storage"),
    'maskdir': os.path.expanduser("~/storage/masks"),
    'resultsroot': os.path.expanduser("/GPFS/pipeline"),
    'filestoreroot': os.path.expanduser("~/sqlite/filestore"),
    'delayed': True,
    'client': None,
    'databases': default_databases,
    'TFLAGS': {'out_dir': '/GPFS/pipeline/ml-tmp',
               'num_batches': 16}
}


delayed = config.get('delayed', _DEFAULTS['delayed'])
storagedir = config.get('storagedir', _DEFAULTS['storagedir'])
maskdir = config.get('maskdir', _DEFAULTS['maskdir'])
resultsroot = config.get('resultsroot', _DEFAULTS['resultsroot'])

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


client = config.get('client', _DEFAULTS['client'])
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
