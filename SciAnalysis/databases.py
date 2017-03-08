'''
    Set up the databases. Changed database_enable flag to change behaviour.
    Eventually this should be in some connection file.

'''
from SciAnalysis.database_initializers import cmsdb, chxdb

database_enable = {
    'chx' : False,
    'cms' : True,
}

database_setups = {
    'cms' : {'initializer' : cmsdb,
             'kwargs' : dict(HOST_DATA='xf11bm-ca1',
                             PORT_DATA=27017,
                             OLDROOT_DATA = ["/GPFS/xf11bm/Pilatus300"],
                             NEWROOT_DATA = "/media/cmslive",
                             HOST_ANALYSIS="localhost",
                             PORT_ANALYSIS=27021),
             },
    'chx' : {'initializer' : chxdb, 'kwargs' : dict()}
}

databases = dict()

for key in database_enable:
    if database_enable[key]:
        initializer = database_setups[key]['initializer']
        kwargs = database_setups[key]['kwargs']
        dbs = initializer(kwargs)
        databases[key] = dict()
        databases[key]['data'] = dbs[0]
        databases[key]['analysis'] = dbs[1]
    else:
        databases[key] = None

