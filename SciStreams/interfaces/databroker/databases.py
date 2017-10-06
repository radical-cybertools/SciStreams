'''
    Set up the databases. Changed database_enable flag to change behaviour.
    Eventually this should be in some connection file.

'''
from .database_initializers import init_db

# this contains dbinfo, which may be read from a scianalysis.yml file
from ... import config

# some handlers
# from filestore.handlers import DATHandler, NpyHandler
from .handlers_custom import PNGHandler
# from .handlers_custom import AreaDetectorTiffHandler

'''
    # register some handlers
    # TODO : allow externally adding handlers
    fs_analysis.register_handler('PNG', PNGHandler, overwrite=True)
    fs_analysis.register_handler('JPG', PNGHandler, overwrite=True)
    fs_analysis.register_handler('DAT', DATHandler, overwrite=True)
    fs_analysis.register_handler('npy', NpyHandler, overwrite=True)
'''

# special handlers for analysis to be setup
# this is hard coded to but should eventually be moved somewhere else
analysis_handlers = {
    'PNG': PNGHandler,
    'JPG': PNGHandler,
    # 'DAT': DATHandler,
    # 'npy': NpyHandler,
}

data_handlers = {
    # 'AD_TIFF': AreaDetectorTiffHandler,
}


# specify as a function to allow re-initialization if needed
# (for sqlite, this can help)
# databases are something like:
# 'chx' : {'data' : {'host' : ..., 'port' : ..., 'fsname' : ...,
#       'mdsname' : ..., ...}, 'analysis' : {...}}
# 'cms' : ...
def initialize(dbname=None):
    databases = dict()
    databases_info = config.databases
    for dbname, subdbs in databases_info.items():
        for subdbname, dbinfo in subdbs.items():
            print("dbname : {}; subdb : {}".format(dbname, subdbname))
            if subdbname == "analysis":
                handlers = analysis_handlers
            elif subdbname == "data":
                handlers = data_handlers
            else:
                # should not in principle get here
                handlers = None
            databases[dbname + ":" + subdbname] = init_db(dbinfo['host'],
                                                          dbinfo['port'],
                                                          dbinfo['mdsname'],
                                                          dbinfo['fsname'],
                                                          handlers=handlers)
    return databases


# TODO : move initialization elsewhere
databases = initialize()
