'''
    Set up the databases. Changed database_enable flag to change behaviour.
    Eventually this should be in some connection file.

'''
from SciAnalysis.interfaces.databroker.database_initializers import init_cmsdb, init_chxdb


def initialize(dbname=None):
    # TODO : allow a string to be specified
    # when these flags set true, enables these databases
    database_enable = {
        'chx' : False,
        'cms' : True,
    }

    database_setups = {
        'cms' : {'initializer' : init_cmsdb,
                 'kwargs' : dict(HOST_DATA='xf11bm-ca1',
                                 PORT_DATA=27017,
                                 ROOTMAP_DATA= {},#{"/GPFS/xf11bm/Pilatus300": "/media/cmslive"},
                                 HOST_ANALYSIS="localhost",
                                 PORT_ANALYSIS=27025,
                                 ROOTMAP_ANALYSIS={}),
                 },
        'chx' : {'initializer' : init_chxdb, 'kwargs' : dict(HOST_DATA='xf11id-srv1',
                                                             PORT_DATA=27017,
                                                             ROOTMAP_DATA= {},
                                                             HOST_ANALYSIS="localhost",
                                                             PORT_ANALYSIS=27021,
                                                             ROOTMAP_ANALYSIS={}

                                                             )
                 }
    }

    databases = dict()

    # enable databases one by one
    # only initialize if not done yet (remove if using sqlite)
    for key in database_enable:
        if database_enable[key]:
            initializer = database_setups[key]['initializer']
            kwargs = database_setups[key]['kwargs']
            dbs = initializer(**kwargs)
            databases[key] = dict()
            databases[key]['data'] = dbs[0]
            databases[key + ":" + "data"] = dbs[0]
            databases[key]['analysis'] = dbs[1]
            databases[key + ":" + "analysis"] = dbs[1]
        else:
            databases[key] = None
    return databases

# TODO : move initialization elsewhere
databases = initialize()
