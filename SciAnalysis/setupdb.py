from metadatastore.mds import MDS
from filestore.fs import FileStore
# MDS and FS could come from different source
from portable_mds.sqlite.mds import MDS as MDS_Analysis
from portable_fs.sqlite.fs import FileStore as FileStore_Analysis

from databroker import Broker 
import tifffile
import numpy as np

#####    Data located in this database
# Port used for tunneling (CMS' mongodb port is 27017)
HOST_DATA = 'xf11bm-ca1'
PORT_DATA = 27017
# old cms root directory for EIGER files and other filestore stuff
# set to blank list (NOT blank string!) to use default
#OLDROOT_DATA = ["/GPFS/xf11bm/Pilatus300"]
OLDROOT_DATA = list()
# new root directory for EIGER files and other filestore stuff
NEWROOT_DATA = "/media/cmslive"

# location of mongodb for analysis, paths defined here just meant to override
# previous path used by filestore (useful when porting)
PORT_ANALYSIS = 27021
OLDPATH_ANALYSIS = []
NEWPATH_ANALYSIS = ""


## SETUP
# Set up of the databroker
# This an example. You'll need to know your local configuration.
mds = MDS({
        #'host': 'xf11bm-ca1',
        'host': HOST_DATA,
             #'port': 27017,
             'port': PORT_DATA,
             'database': 'metadatastore-production-v1',
             'timezone': 'US/Eastern',
             }, auth=False)
# This an example. You'll need to know your local configuration.
fs = FileStore({
        #'host': 'xf11bm-ca1',
        'host': 'localhost',
                  #'port': 27017,
                  'port': PORT_DATA,
                  'database': 'filestore-production-v1'})

cmsdb = Broker(mds, fs)
print("Set up the cms database at `cmsdb`. Please test if connection is"
      "successful by running chxdb[-1]")

# this one is made specifically for the AreaDetectorTiffHandler
def changerootdir(oldroots, newroot):
    ''' returns a decorator that acts on function in a class.
        changes substring oldroots to newroot in filepaths
    '''
    def f_outer(f):
        def f_inner(self, fpath, template, filename, frame_per_point=1):
            for oldroot in oldroots:
                fpath = fpath.replace(oldroot, newroot)
            return f(self, fpath, template, filename, frame_per_point=1)
        return f_inner
    return f_outer

from filestore.handlers_base import HandlerBase
class AreaDetectorTiffHandler(HandlerBase):
    specs = {'AD_TIFF'} | HandlerBase.specs

    @changerootdir(OLDROOT_DATA, NEWROOT_DATA)
    def __init__(self, fpath, template, filename, frame_per_point=1):
        self._path = fpath
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename

    def _fnames_for_point(self, point_number):
        start, stop = point_number * self._fpp, (point_number + 1) * self._fpp
        for j in range(start, stop):
            yield self._template % (self._path, self._filename, j)

    def __call__(self, point_number):
        ret = []
        for fn in self._fnames_for_point(point_number):
            with tifffile.TiffFile(fn) as tif:
                ret.append(tif.asarray())
        return np.array(ret).squeeze()

    def get_file_list(self, datum_kwargs):
        ret = []
        for d_kw in datum_kwargs:
            ret.extend(self._fnames_for_point(**d_kw))
        return ret

cmsdb.fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)

### ANALYSIS STORE SETUP
''' Instructions on how to port this to actual database:
    1. remove where it says 'test directory' and 'test path'
    2. make sure the database field is correct
    3. change MDS_Analysis -> MDS and FileStore_Analyis -> FileStore
    WARNING: Make sure there is *no conflict* in the database name!

'''
# Set up of the databroker
# This an example. You'll need to know your local configuration.
mds_analysis_conf = {
                     'host': 'localhost',
                     'port': PORT_ANALYSIS,
                     # uses metadatastore
                     'database': 'metadatastore-production-v1',
                     'timezone': 'US/Eastern',
                     # test directory
                     'directory' : '/home/group/mongodb-cms/sqlite-db'
                     }
#mds_analysis = MDS_Analysis(mds_analysis_conf, auth=False)
# This an example. You'll need to know your local configuration.
fs_analysis_conf = {
                    'host': 'localhost',
                    'port': PORT_ANALYSIS,
                    'database': 'filestore-production-v1',
                    # test path
                     'dbpath' : '/home/group/mongodb-cms/sqlite-db.db'
                    }

# if first time, run this:
#from filestore.utils import install_sentinels
#install_sentinels(fs_analysis_conf, version_number)
#fs_analysis = FileStore_Analysis(fs_analysis_conf)

#cmsdb_analysis = Broker(mds_analysis, fs_analysis)
#print("Set up the cms analysis database at `cmsdb`. Please test if connection is"
      #"successful by running chxdb[-1]")
