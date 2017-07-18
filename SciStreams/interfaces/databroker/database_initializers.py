# Create Databases
# NOTE : cmsdb makes temporary analysis database for now...
from metadatastore.mds import MDS
from filestore.fs import FileStore
from databroker.broker import Broker

''' This handles setting up the databroker database.

    Some notes:
    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)

'''


def init_db(host, port, mdsname, fsname, handlers=None):
    ''' Initialize a database.
        This is the general function used in SciStreams to handle this

        Need to supply a host, port, mdsname and fsname

        Parameters
        ----------

        host : the host ip address (usually localhost)
        port : the port to run on
        mdsname : the database name for the metadatastore
        fsname : the database name for the filestore

        handlers : a dictionary of handlers of format
            key : handler function

        Returns
        -------
        The database

        Notes
        -----
        The returned object contains open sockets. It cannot be pickled.
        It is recommended to be initialized in a local library for distributed
        computing.
    '''
    mds_conf = {
                'host': host,
                'port': port,
                'database': mdsname,
                'timezone': 'US/Eastern',
                 }
    mds = MDS(mds_conf, auth=False)

    fs_conf = {
            'host': host,
            'port': port,
            'database': fsname
    }

    fs = FileStore(fs_conf)  # , root_map=ROOTMAP)

    if handlers is not None:
        for handler_key, handler_function in handlers.items():
            fs.register_handler(handler_key, handler_function, overwrite=True)

    db = Broker(mds, fs)

    return db
