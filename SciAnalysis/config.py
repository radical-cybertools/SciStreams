import yaml
import os.path
# reads yaml file from user directory
filename = os.path.expanduser("~/.config/scianalysis/scianalysis.yml")
f = open(filename)
config = yaml.load(f)

_DEFAULTS = {
    'delayed' : True,
    'storagedir' : "../storage",
    'maskdir' : "masks",
    'resultsroot' : os.path.expanduser("~/sqlite"),
    'xmldir' : "xml-files",
    'delayed' : True,
    'client' : None
}


delayed = config.get('delayed', _DEFAULTS['delayed'])
storagedir = config.get('storagedir', _DEFAULTS['storagedir'])
maskdir = config.get('maskdir', _DEFAULTS['maskdir'])
maskdir = storagedir + "/" + maskdir
resultsroot = config.get('resultsroot', _DEFAULTS['resultsroot'])
xmldir = config.get('xmldir', _DEFAULTS['xmldir'])
xmldir = storagedir + "/" + xmldir

client = config.get('client', _DEFAULTS['client'])


if delayed:
    from dask import delayed
else:
    def delayed(pure=None, pure_default=None):
        def dec(f):
            def fnew(*args, **kwargs):
                return f(*args, **kwargs)
            return fnew
        return dec
