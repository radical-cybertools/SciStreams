import yaml
import os.path
# reads yaml file from user directory
filename = os.path.expanduser("~/.config/scianalysis/scianalysis.yml")
try:
    f = open(filename)
    config = yaml.load(f)
except FileNotFoundError:
    config = dict()

detector_names = dict(pilatus300='saxs', psccd='waxs')

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
    'resultsroot' : os.path.expanduser("/GPFS/pipeline"),
    'filestoreroot' : os.path.expanduser("~/sqlite/filestore"),
    'delayed' : True,
    'client' : None,
    'databases' : default_databases
}


delayed = config.get('delayed', _DEFAULTS['delayed'])
storagedir = config.get('storagedir', _DEFAULTS['storagedir'])
maskdir = config.get('maskdir', _DEFAULTS['maskdir'])
maskdir = storagedir + "/" + maskdir
resultsroot = config.get('resultsroot', _DEFAULTS['resultsroot'])
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
