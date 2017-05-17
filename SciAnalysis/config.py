import yaml
import os.path
# reads yaml file from user directory
filename = os.path.expanduser("~/.config/scianalysis/scianalysis.yml")
f = open(filename)
config = yaml.load(f)

#for data
cmsdb_data=dict(
host = "localhost",
port=27017,
mdsname='metadatastore-production-v1',
fsname = "filestore-production-v1",
)

# for analysis
cmsdb_analysis = dict(
host = "localhost",
port=27025,
mdsname = "analysis-metadatastore-v1",
fsname = "analysis-filestore-v1",
)

default_databases = dict(cms=dict(analysis=cmsdb_analysis, data=cmsdb_data))




_DEFAULTS = {
    'delayed' : True,
    'storagedir' : "../storage",
    'maskdir' : "masks",
    'resultsroot' : os.path.expanduser("~/sqlite"),
    'xmldir' : "xml-files",
    'delayed' : True,
    'client' : None,
    'databases' : default_databases
}


delayed = config.get('delayed', _DEFAULTS['delayed'])
storagedir = config.get('storagedir', _DEFAULTS['storagedir'])
maskdir = config.get('maskdir', _DEFAULTS['maskdir'])
maskdir = storagedir + "/" + maskdir
resultsroot = config.get('resultsroot', _DEFAULTS['resultsroot'])
xmldir = config.get('xmldir', _DEFAULTS['xmldir'])
xmldir = storagedir + "/" + xmldir

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
