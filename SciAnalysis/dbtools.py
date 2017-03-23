# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails
import time
from uuid import uuid4
import numpy as np
from databroker.broker import Header
import json
from writers_custom import writers_dict as _writers_dict

_ANALYSIS_STORE_VERSION = 'beta-v1'


def safe_parse_databroker(val, nested=False):
    ''' Parse an arg, make sure it's safe for databroker.'''
    if isinstance(val, dict):
        for key, subval in val.items():
            val[key] = safe_parse_databroker(subval,nested=True)
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
        val = json.dumps(val)

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

def store_results_databroker(results, attributes, dbname, external_writers={}):
    ''' Save results to a databroker instance.'''
    import SciAnalysis.databases as dblib
    # TODO : remove this when in mongodb
    databases = dblib.initialize()
    # TODO : check for time out on database access, return an erorr tha tmakes sense
    db = databases[dbname]['analysis']
    # saving to databroker
    mds = db.mds # metadatastore

    # Store in databroker, make the documents
    start_doc = dict()

    start_doc.update(attributes)

    start_doc['time'] = time.time()
    start_doc['uid'] = str(uuid4())
    start_doc['plan_name'] = 'analysis'
    start_doc['protocol_name'] = results['_name']
    start_doc['start_timestamp'] = results['_run_stats']['start_timestamp']
    start_doc['end_timestamp'] = results['_run_stats']['end_timestamp']
    start_doc['runtime'] = start_doc['start_timestamp'] - start_doc['end_timestamp']
    start_doc['save_timestamp'] = time.time()
    start_doc['_output_names'] = results['_output_names']
    # TODO : replace with version lookup in database
    start_doc['analysis_store_version'] = _ANALYSIS_STORE_VERSION

    if '_run_args' in results:
        results['_run_args'] = safe_parse_databroker(results['_run_args'])
        start_doc['run_args'] = results['_run_args']

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
    for key, val in results.items():
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

    if '_files' not in results:
        results['_files'] = dict()

    # NOTE : This is an alternative option. User can write
    # files themselves and specify that the file was written
    # then parse files, val is a dict
    # if files were saved, store info in filestore
    if '_files' in results:
        for key, val in results['_files'].items():
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
