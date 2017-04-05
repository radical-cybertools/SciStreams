# Create Databases
# NOTE : cmsdb makes temporary analysis database for now...
from metadatastore.mds import MDS
from filestore.fs import FileStore
from filestore.handlers import DATHandler, NpyHandler
from portable_mds.sqlite.mds import MDS as MDS_SQLITE
from portable_fs.sqlite.fs import FileStore as FileStore_SQLITE
from databroker.broker import Broker
import tifffile
import numpy as np
import tempfile
import os
from functools import partial
from .handlers_custom import PNGHandler


def init_cmsdb(HOST_DATA='xf11bm-ca1', PORT_DATA=27017,
          ROOTMAP_DATA = {"/GPFS/xf11bm/Pilatus300": "/media/cmslive"},
          HOST_ANALYSIS="localhost", PORT_ANALYSIS=27017,
          ROOTMAP_ANALYSIS = dict()):
    ''' Setup the database.
        HOST_DATA : the host for the data
        PORT_DATA : the port of the data
        ROOTMAP_DATA : the map of strings to new path strings for FileStore
            for the data database
        HOST_ANALYSIS : the host for the analysis
        PORT_ANALYSIS : the port of the analysis
        ROOTMAP_ANALYSIS : the map of strings to new path strings for FileStore
            for the analysis database
    '''
    
    cmsdb_data = init_cmsdb_data(HOST=HOST_DATA, PORT=PORT_DATA,
                                 ROOTMAP=ROOTMAP_DATA)

    from SciAnalysis.interfaces.databroker.handlers_custom import AreaDetectorTiffHandler  
    cmsdb_data.fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)


    cmsdb_analysis = init_cmsdb_anal_tmp(HOST=HOST_ANALYSIS, PORT=PORT_ANALYSIS,
                                         ROOTMAP=ROOTMAP_ANALYSIS)
    
    print("Set up the cms analysis database at `cmsdb`. Please test if connection is"
          "successful by running chxdb[-1]")

    return cmsdb_data, cmsdb_analysis


def init_cmsdb_data(HOST='xf11bm-ca1', PORT=27017,
               ROOTMAP= {"/GPFS/xf11bm/Pilatus300": "/media/cmslive"}):
    ''' Setup the database.
        HOST_DATA : the host for the data
        PORT_DATA : the port of the data
        ROOTMAP_DATA : the map of strings to new path strings for FileStore
            for the data database
        HOST_ANALYSIS : the host for the analysis
        PORT_ANALYSIS : the port of the analysis
        ROOTMAP_ANALYSIS : the map of strings to new path strings for FileStore
            for the analysis database
    '''
    mds = MDS({
            #'host': 'xf11bm-ca1',
            'host': HOST,
                 #'port': 27017,
                 'port': PORT,
                 'database': 'metadatastore-production-v1',
                 'timezone': 'US/Eastern',
                 }, auth=False)

    fs = FileStore({
            #'host': 'xf11bm-ca1',
            'host': HOST,
                      #'port': 27017,
            'port': PORT,
            'database': 'filestore-production-v1'}, root_map=ROOTMAP)
    
    cmsdb_data = Broker(mds, fs)
    print("Set up the cms database at `cmsdb`. Please test if connection is"
          "successful by running chxdb[-1]")
    return cmsdb_data



def init_cmsdb_anal(HOST="localhost",
          PORT=27021,
          ROOTMAP={}):
    ''' Creates connection to analysis database.'''
    ### ANALYSIS STORE SETUP
    mds_analysis = MDS({
            'host': HOST,
                 'port': PORT,
                 'database': 'metadatastore-production-v1',
                 'timezone': 'US/Eastern',
                 }, auth=False)

    fs_analysis_conf = {
            'host': HOST,
            'port': PORT,
            'database': 'filestore-production-v1'}
    
    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)
    fs_analysis = FileStore(fs_analysis_conf, root_map=ROOTMAP)
    
    cmsdb_analysis = Broker(mds_analysis, fs_analysis)


# HARD CODED temporary database
def init_cmsdb_anal_tmp(HOST="localhost",
          PORT=27021,
          ROOTMAP={}):
    ''' this one just ignores and creates sandbox anal db.
        for temporary use
    '''
    #tmpfile = tempfile.NamedTemporaryFile().name
    #tmpdir_analysis = tempfile.TemporaryDirectory().name
    #os.mkdir(tmpdir_analysis)
    #tmpdir_data = tempfile.TemporaryDirectory().name
    #os.mkdir(tmpdir_data)
    tmpfile = "/home/lhermitte/sqlite/cmsdb_analysis/cmsdb_analysis.tmp"
    tmpdir_analysis = "/home/lhermitte/sqlite/cmsdb_analysis"
    tmpdir_data = "/home/lhermitte/sqlite/cmsdb_data"
    

    mds_analysis_conf = {
                         'database': 'metadatastore-production-v1',
                         'timezone': 'US/Eastern',
                         # test directory
                         'directory' : tmpdir_analysis
                         }

    mds_analysis = MDS_SQLITE(mds_analysis_conf, auth=False)
    # This an example. You'll need to know your local configuration.
    fs_analysis_conf = {
                        'database': 'filestore-production-v1',
                        # test path
                         'dbpath' : tmpfile
                        }
    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)
    fs_analysis = FileStore_SQLITE(fs_analysis_conf)
    fs_analysis.register_handler('PNG', PNGHandler, overwrite=True)
    fs_analysis.register_handler('JPG', PNGHandler, overwrite=True)
    fs_analysis.register_handler('DAT', DATHandler, overwrite=True)
    fs_analysis.register_handler('npy', NpyHandler, overwrite=True)

    cmsdb_analysis = Broker(mds_analysis, fs_analysis)

    return cmsdb_analysis

def init_chxdb():
    return None
