from metadatastore.mds import MDS
from filestore.fs import FileStore
from databroker import Broker
import tifffile
import numpy as np

def cmsdb(HOST_DATA='xf11bm-ca1', PORT_DATA=27017,
          OLDROOT_DATA=["/GPFS/xf11bm/Pilatus300"],
          NEWROOT_DATA="/media/cmslive",
          OLDPATH_ANALYSIS = [],
          NEWPATH_ANALYSIS = "",
          HOST_ANALYSIS="localhost",
          PORT_ANALYSIS=27021):
    ''' Setup the database.
        HOST_DATA : the host for the data
        PORT_DATA : the port of the data
        OLDROOT_DATA : the old string for the root(s) of data
        NEWROOT_DATA : the new root path for the data
        OLDPATH_ANALYSIS : the old path for analysis
        NEWPATH_ANALYSIS : the new path for analysis
        HOST_ANALYSIS : the host for the analysis
        PORT_ANALYSIS : the port of the analysis
    '''
    mds = MDS({
            #'host': 'xf11bm-ca1',
            'host': HOST_DATA,
                 #'port': 27017,
                 'port': PORT_DATA,
                 'database': 'metadatastore-production-v1',
                 'timezone': 'US/Eastern',
                 }, auth=False)

    fs = FileStore({
            #'host': 'xf11bm-ca1',
            'host': HOST_DATA,
                      #'port': 27017,
            'port': PORT_DATA,
            'database': 'filestore-production-v1'})
    
    cmsdb = Broker(mds, fs)
    print("Set up the cms database at `cmsdb`. Please test if connection is"
          "successful by running chxdb[-1]")
    
    from filestore.handlers_base import HandlerBase
    class AreaDetectorTiffHandler(HandlerBase):
        specs = {'AD_TIFF'} | HandlerBase.specs
    
        def __init__(self, fpath, template, filename, frame_per_point=1):
            # This is hard coded change for CMS Area detector
            for oldroot in OLDROOT_DATA:
                fpath = fpath.replace(oldroot, NEWROOT_DATA)
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
    mds_analysis = MDS({
            'host': HOST_ANALYSIS,
                 'port': PORT_ANALYSIS,
                 'database': 'metadatastore-production-v1',
                 'timezone': 'US/Eastern',
                 }, auth=False)

    fs_analysis_conf = {
            'host': HOST_ANALYSIS,
            'port': PORT_ANALYSIS,
            'database': 'filestore-production-v1'}
    
    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)
    fs_analysis = FileStore(fs_analysis_conf)
    
    cmsdb_analysis = Broker(mds_analysis, fs_analysis)
    print("Set up the cms analysis database at `cmsdb`. Please test if connection is"
          "successful by running chxdb[-1]")
    return cmsdb, cmsdb_analysis

def chxdb():
    return None
