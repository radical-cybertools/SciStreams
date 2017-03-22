# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails

def parse_args_databroker(argsdict):
    ''' Parse args and make sure they are databroker friendly.
        Also useful for hashing the args.
        For ex: if it's a matplotlib instance etc just ignore

        Modifies in place.

        Warning : This is a recursive function
    '''
    for key, val in argsdict.items():
        if isinstance(val, dict):
            parse_args_databroker(val)
        elif isinstance(val, np.ndarray):
            # do nothing
            pass
        elif np.isscalar(val):
            # convenient to check if it's a number
            pass
        else:
            argsdict[key] = str(type(val))
            

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

def store_results_databroker(results, attributes, name, protocol, db):
    ''' Save results to a databroker instance.'''
    # saving to databroker
    mds = db.mds # metadatastore

    # Store in databroker, make the documents
    start_doc = dict()

    start_doc.update(attributes)

    start_doc['time'] = time.time()
    start_doc['uid'] = str(uuid4())
    start_doc['plan_name'] = 'analysis'
    start_doc['name'] = protocol._name
    start_doc['start_timestamp'] = protocol.start_timestamp
    start_doc['end_timestamp'] = protocol.end_timestamp
    start_doc['runtime'] = protocol.end_timestamp - protocol.start_timestamp
    start_doc['save_timestamp'] = time.time()
    start_doc['analysis_store_version'] = _ANALYSIS_STORE_VERSION

    if '_run_args' in results:
        parse_args_databroker(results['_run_args'])
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

    # first parse data
    for key, val in results.items():
        if key[0] == '_':
            continue # ignore hidden keys
        # guess descriptor from data
        descriptor_doc['data_keys'][key] = make_descriptor(val)
        event_doc['data'][key] = val
        event_doc['timestamps'][key] = time.time()

    # then parse files, val is a dict
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

