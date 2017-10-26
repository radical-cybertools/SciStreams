# Create Databases
# NOTE : cmsdb makes temporary analysis database for now...
from databroker import Broker
from databroker.headersource.mongo import MDSRO
from databroker.assets.mongo import RegistryRO


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

    reg_conf = {
            'host': host,
            'port': port,
            'database': fsname
    }

    mds = MDSRO(mds_conf)
    reg = RegistryRO(reg_conf)

    if handlers is not None:
        for handler_key, handler_function in handlers.items():
            reg.register_handler(handler_key, handler_function, overwrite=True)

    db = Broker(mds, reg)

    return db
