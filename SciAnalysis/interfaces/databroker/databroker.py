# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails
import time
from uuid import uuid4
import numpy as np
from databroker.broker import Header
import json

from SciAnalysis.interfaces.databroker.writers_custom import writers_dict as _writers_dict
from SciAnalysis.interfaces.SciResult import SciResult

_ANALYSIS_STORE_VERSION = 'beta-v1'
# TODO : Ask Dan if databroker is smart enough to know if connection was already made?
# for ex: running Broker(config) multiple times, should not recreate connection
# I am thinking of in distributed regime where multiple nodes will be running
# some of them may have already started a db conection, others not


def Header2SciResult(header, db=None):
    ''' Convert a header to a SciResult. '''
    # TODO : write
    scires =  SciResult()
    scires['attributes'] = dict(**header['start'])
    scires['attributes']['data_uid'] = scires['attributes']['uid']
    scires['output_names'] = list(header['descriptors'][0]['data_keys'].keys())
    # TODO : pass conf information for database instead and reconstruct here
    if db is None:
        raise ValueError("Error, need to specify db")
    event = list(db.get_events(header, fill=True))[0]
    output_names = scires['output_names']
    for output_name in output_names:
        scires['outputs'][output_name] = event['data'][output_name]
    scires['run_stats'] = dict() # no run stats for a conversion
    return scires

'''
    This routine looks up the last entry of a certain type
'''
def pullrecent(dbname, protocol_name=None, **kwargs):
    ''' Pull from a databroker database

        Parameters
        ----------

        dbname : str
            Input name is of format "dbname:subdbname"
            For example : "cms:data", or "cms:analysis"
            if just database name supplied, then it's assumed
            to be analysis.

        Returns
        -------
        SciResult of data

    '''
    # Returns a SciResult Basically the SciResult constructor for databroker
    from SciAnalysis.interfaces.databroker.databases import initialize
    # TODO : Remove the initialization when moving from sqlite to other
    if ":" in dbname:
        dbname = dbname.split(":")
    else:
        dbname = [dbname, 'analysis']
    dbs = initialize()
    db = dbs[dbname[0]][dbname[1]]
    kwargs['protocol_name'] = protocol_name
    # search and get latest
    headers = db(**kwargs)
    if len(headers) > 0:
        header = headers[0]
    else:
        raise IndexError("Error, no headers found for database lookup")
    scires = Header2SciResult(header, db=db)

    return scires

def pull(dbname, protocol_name=None, **kwargs):
    ''' Pull from a databroker database
        keeps yielding results until exhausted

        Parameters
        ----------

        dbname : str
            Input name is of format "dbname:subdbname"
            For example : "cms:data", or "cms:analysis"
            if just database name supplied, then it's assumed
            to be analysis.

        Returns
        -------
        SciResult of data

    '''
    # Returns a SciResult Basically the SciResult constructor for databroker
    from SciAnalysis.interfaces.databroker.databases import initialize
    # TODO : Remove the initialization when moving from sqlite to other
    if ":" in dbname:
        dbname = dbname.split(":")
    else:
        dbname = [dbname, 'analysis']
    dbs = initialize()
    db = dbs[dbname[0]][dbname[1]]
    kwargs['protocol_name'] = protocol_name
    # search and get latest
    headers = db(**kwargs)

    for header in headers:
        try:
            scires = Header2SciResult(header, db=db)
        except FileNotFoundError:
            continue
        yield scires

def safe_parse_databroker(val, nested=False):
    ''' Parse an arg, make sure it's safe for databroker.'''
    if isinstance(val, dict):
        for key, subval in val.items():
            val[key] = safe_parse_databroker(subval, nested=True)
    elif isinstance(val, tuple):
        newval = tuple([safe_parse_databroker(v,nested=True) for v in val])
        val = newval
    elif isinstance(val, list):
        newval = list([safe_parse_databroker(v,nested=True) for v in val])
        val = newval
    elif isinstance(val, np.ndarray):
        # do nothing
        pass
    elif np.isscalar(val):
        # convenient to check if it's a number
        pass
    elif isinstance(val, Header):
        val = dict(val)
    else:
        val = str(type(val))

    if not nested:
        try:
            val = json.dumps(val)
        except TypeError:
            val = json.dumps(repr(val))

    return val


def make_descriptor(val):
    ''' make a descriptor from value through guessing.'''
    shape = ()
    if np.isscalar(val):
        dtype = 'number'
    elif isinstance(val, np.ndarray):
        dtype = 'array'
        shape = val.shape
    elif isinstance(val, list):
        dtype = 'list'
        shape = (len(val),)
    elif isinstance(val, dict):
        dtype = 'dict'
    else:
        dtype = 'unknown'

    return dict(dtype=dtype, shape=shape)

# a decorator
def store_results(dbname, external_writers={}):
    def decorator(f):
        def newf(*args, **kwargs):
            import SciAnalysis.interfaces.databroker.databroker as source_databroker
            results = f(*args, **kwargs)
            # TODO : fill in (after working on xml storage)
            attributes = {}
            print(dbname)
            source_databroker.store_results_databroker(results, dbname, external_writers=external_writers)
            return results
        return newf
    return decorator

def store_results_databroker(scires, dbname, external_writers={}):
    ''' Save results to a databroker instance.
        Takes a sciresult instance.
    '''
    import SciAnalysis.interfaces.databroker.databases as dblib
    # TODO : remove this when in mongodb
    databases = dblib.initialize()
    # TODO : check for time out on database access, return an erorr tha tmakes sense
    if ":" in dbname:
        dbname, dbsubname = dbname.split(":")
    else:
        dbsubname = 'analysis'
    db = databases[dbname][dbsubname]
    # saving to databroker
    mds = db.mds # metadatastore

    # Store in databroker, make the documents
    start_doc = dict()

    #start_doc.update(attributes)

    start_doc.update(**scires['attributes'])
    start_doc['time'] = time.time()
    start_doc['uid'] = str(uuid4())
    start_doc['plan_name'] = 'analysis'
    #start_doc['start_timestamp'] = scires['run_stats']['start_timestamp']
    #start_doc['end_timestamp'] = scires['run_stats']['end_timestamp']
    start_doc['run_stats'] = scires['run_stats']
    #start_doc['runtime'] = start_doc['start_timestamp'] - start_doc['end_timestamp']
    start_doc['save_timestamp'] = time.time()
    start_doc['output_names'] = scires['output_names']
    # TODO : replace with version lookup in database
    start_doc['analysis_store_version'] = _ANALYSIS_STORE_VERSION

    #if '_run_args' in scires:
        #results['_run_args'] = safe_parse_databroker(results['_run_args'])
        #start_doc['run_args'] = results['_run_args']

    # just make one descriptor and event document for now
    # initialize both event and descriptor
    descriptor_doc = dict()
    event_doc = dict()
    event_doc['data'] = dict()
    event_doc['timestamps'] = dict()
    descriptor_doc['data_keys'] = dict()
    descriptor_doc['time'] = time.time()
    descriptor_doc['uid'] = str(uuid4())
    descriptor_doc['run_start'] = start_doc['uid']
    event_doc['time'] = time.time()
    event_doc['uid'] = str(uuid4())
    event_doc['descriptor'] = descriptor_doc['uid']
    event_doc['seq_num'] = 1

    # then parse remaining data
    for key, val in scires['outputs'].items():
        if key[0] == '_':
            continue # ignore hidden keys
        # guess descriptor from data
        descriptor_doc['data_keys'][key] = make_descriptor(val)
        # save to filestore
        if key in external_writers:
            writer_key = external_writers[key]
            writer = _writers_dict[writer_key](db.fs)
            # TODO : Move this assumption of file path elsewhere?
            time_now = time.localtime()
            subpath = "/{:04}/{:02}/{:02}".format(time_now.tm_year, time_now.tm_mon, time_now.tm_mday)
            new_id = writer.write(val, subpath=subpath)
            event_doc['data'][key] = new_id
            descriptor_doc['data_keys'][key].update(external="FILESTORE:")
        else:
            event_doc['data'][key] = safe_parse_databroker(val)
        event_doc['timestamps'][key] = time.time()

    # TODO : decide if we do need a feature to give filenames
    # NOTE : This is an alternative option. User can write
    # files themselves and specify that the file was written
    # then parse files, val is a dict
    # if files were saved, store info in filestore
    if '_files' in scires:
        for key, val in scires['_files'].items():
            datum, desc = parse_file_event(val, db)
            descriptor_doc['data_keys'][key] = desc
            event_doc['data'][key] = datum
            event_doc['timestamps'][key] = time.time()


    stop_doc = dict()
    stop_doc['time'] = time.time()
    stop_doc['uid'] = str(uuid4())
    stop_doc['run_start'] = start_doc['uid']
    stop_doc['exit_status'] = 'success'

    mds.insert('start', start_doc)
    mds.insert('descriptor', descriptor_doc)
    mds.insert('event', event_doc)
    mds.insert('stop', stop_doc)

    
def parse_file_event(entry, db):
    ''' Parse a file event descriptor (our custom descriptor),
        and translate into a datum (could be uid, or actual data)
        and a datum_dict (dictionary descriptor for the datum)

        Returns
        -------
        datum : the result 
        datum_dict : the dictionary describing the result
    '''
    dat_dict = dict()
    if 'dtype' in entry:
        dat_dict['dtype'] = entry['dtype']
    if 'shape' in entry:
        dat_dict['shape'] = entry['shape']
    if 'source' in entry:
        dat_dict['source'] = entry['source']
    if 'external' in entry:
        dat_dict['external'] = entry['external']
    if 'filename' in entry:
        dat_dict['filename'] = entry['filename']

    # this is for filestore instance
    if 'filename' in entry:
        fs = db.fs # get filestore
        # make sure it's absolute path
        filename = os.path.abspath(os.path.expanduser(entry['filename']))
        dat_uid = str(uuid4())
        # try to guess some parameters here
        if 'spec' in entry:
            spec = entry['spec']
        else:
            extension = os.path.splitext(filename)[1]
            if len(extension) > 1:
                spec = extension[1:].upper()
            else:
                raise ValueError("Error could not figure out file type for {}".format(filename))
        entry.setdefault('resource_kwargs', {})
        entry.setdefault('datum_kwargs', {})
        resource_kwargs = entry['resource_kwargs']
        datum_kwargs = entry['datum_kwargs']
        # could also add datum_kwargs
        # databroker : two step process: 1. insert resource 2. Save data
        resource_document = fs.insert_resource(spec, filename, resource_kwargs)
        fs.insert_datum(resource_document, dat_uid, datum_kwargs)
        # overwrite with correct argument
        dat_dict['external'] = "FILESTORE:"

    return dat_uid, dat_dict
