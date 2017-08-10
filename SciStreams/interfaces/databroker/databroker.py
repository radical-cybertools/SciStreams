# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails
import time
from uuid import uuid4
import numpy as np
import matplotlib

from databroker.broker import Header
import json

from .writers_custom \
        import writers_dict as _writers_dict
from ..StreamDoc import StreamDoc
from metadatastore.core import NoEventDescriptors

# TODO : change to the new databroker version but leave for now
#from databroker.eventsource.shim import EventSourceShim
#NoEventDescriptors = EventSourceShim.NoEventDescriptors

matplotlib.use("Agg")


_ANALYSIS_STORE_VERSION = 'beta-v2'
# TODO : Ask Dan if databroker is smart enough to know if connection was
# already made?  for ex: running Broker(config) multiple times, should not
# recreate connection I am thinking of in distributed regime where multiple
# nodes will be running some of them may have already started a db conection,
# others not


def Header2StreamDoc(header, dbname="cms:data", fill=True):
    ''' Convert a header to a StreamDoc.

        Note: This assumes header contains only one event.
            Need to add to function if dealing with multiple events.
    '''
    sdoc = StreamDoc()
    attributes = header['start'].copy()
    attributes['data_uid'] = attributes['uid']
    sdoc.add(attributes=attributes)

    from .databases import databases

    db = databases[dbname]

    # Assume first event
    try:
        event = list(db.get_events(header, fill=fill))[0]
        eventdata = event['data']
    except IndexError:
        # there are no events
        print("Found no events")
        eventdata = []
    except KeyError:
        print("Event was corrupt")
        eventdata = []

    sdoc.add(kwargs=eventdata)

    return sdoc


'''
    Useful routines for searching of databroker items.
'''


def pullrecent(dbname, protocol_name=None, **kwargs):
    ''' Pull from a databroker database

        This routine looks up the last entry of a certain type

        Parameters
        ----------

        dbname : str
            Input name is of format "dbname:subdbname"
            For example : "cms:data", or "cms:analysis"
            if just database name supplied, then it's assumed
            to be analysis.

        Returns
        -------
        StreamDoc of data

    '''
    from .databases import databases
    # TODO : Remove the initialization when moving from sqlite to other
    # (sqlite requires db to be initialized every time... but db it can
    # be a running instance in the imported library for that process)
    db = databases[dbname]
    kwargs['protocol_name'] = protocol_name
    # search and get latest
    headers = db(**kwargs)
    if len(headers) > 0:
        header = headers[0]
    else:
        raise IndexError("Error, no headers found for database lookup")
    sdoc = Header2StreamDoc(header, dbname=dbname)

    return sdoc


def pullfromuid(uid, dbname=None):
    ''' Pull from a databroker database from a uid

        Parameters
        ----------

        dbname : str
            Input name is of format "dbname:subdbname"
            For example : "cms:data", or "cms:analysis"
            if just database name supplied, then it's assumed
            to be analysis.

        uid : the uid of dataset

        Returns
        -------
        StreamDoc of data

    '''
    if dbname is None:
        raise ValueError("Error must supply a dbname")
    from .databases import databases
    # TODO : Remove the initialization when moving from sqlite to other
    # (sqlite requires db to be initialized every time... but db it can
    # be a running instance in the imported library for that process)
    db = databases[dbname]
    # search and get latest
    if uid is None:
        raise ValueError("Need to specify a uid")

    # TODO : keep up to date on issue databroker/#151 to see if this is
    # resolved
    # (so I can just dict(db[uid]), or not dict at all (if they make header
    # serializable)
    header = dict(db[uid])

    scires = Header2StreamDoc(header, dbname)

    return scires


def pullfromuids(dbname, uids):
    for uid in uids:
        yield pullfromuid(dbname, uid)


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

        protocol_name : the protocol name used (if analysis database)

        kwargs : entries in metadatabase to search for. searches for exact
        matches. See search for searching for substrings

        Returns
        -------
        StreamDoc of data

    '''
    # Returns a StreamDoc Basically the StreamDoc constructor for databroker
    from .databases import databases
    # TODO : Remove the initialization when moving from sqlite to other
    dbs = databases
    db = dbs[dbname]
    if protocol_name is not None:
        kwargs['protocol_name'] = protocol_name
    # search and get latest
    headers = db(**kwargs)

    for header in headers:
        try:
            sdoc = Header2StreamDoc(header, dbname=dbname)
            # print('got item')
        except FileNotFoundError:
            print('Warning (databroker) : File not found')
            continue
        except NoEventDescriptors:
            print('Warning (databroker) : no event desc')
            continue
        except IndexError:  # no events
            print('Warning (databroker) : index error')
            continue

        yield sdoc


def search(dbname, start_time=None, stop_time=None, **kwargs):
    ''' search database for a substring in one of the fields.

        TODO : allow start and stop times to be numbers (number of seconds
        before now)

    '''
    # Returns a StreamDoc Basically the StreamDoc constructor for databroker
    from .databases import databases
    # TODO : Remove the initialization when moving from sqlite to other

    db = databases[dbname]

    if start_time is None or stop_time is None:
        print("Warning, please select a start or stop time (or else this"
              " would just take forever")

    headers = db(start_time=start_time, stop_time=stop_time)
    for header in headers:
        start_doc = header['start']
        found = True
        for key, val in kwargs.items():
            if key not in start_doc:
                found = False
            elif val not in start_doc[key]:
                found = False
        if found:
            try:
                sdoc = Header2StreamDoc(header, dbname)
                yield sdoc
            except FileNotFoundError:
                continue
            except StopIteration:
                yield StopIteration


def safe_parse_databroker(val, nested=False):
    ''' Parse an arg, make sure it's safe for databroker.
        Also, if it's a huge numpy array, it'll be truncated.
            Big arrays shouldn't be here.
    '''
    if isinstance(val, dict):
        for key, subval in val.items():
            val[key] = safe_parse_databroker(subval, nested=True)
    elif isinstance(val, tuple):
        newval = tuple([safe_parse_databroker(v, nested=True) for v in val])
        val = newval
    elif isinstance(val, list):
        newval = list([safe_parse_databroker(v, nested=True) for v in val])
        val = newval
    elif isinstance(val, np.ndarray):
        # don't print the full np array to databroker
        val = repr(val)
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


def store_results_databroker(sdoc, dbname=None, external_writers={}):
    ''' Save results to a databroker instance.
        Takes a streamdoc instance.
    '''
    if dbname is None:
        raise ValueError("No database selected. Cancelling.")
    # TODO : change this when in mongodb
    from .databases import databases
    # TODO : check for time out on database access, return an erorr that makes
    # sense
    db = databases[dbname]

    # saving to databroker
    mds = db.mds  # metadatastore

    # Store in databroker, make the documents
    start_doc = dict()

    # start_doc.update(attributes)

    start_doc.update(**sdoc['attributes'])
    start_doc['time'] = time.time()
    start_doc['uid'] = str(uuid4())
    start_doc['plan_name'] = 'analysis'
    start_doc['run_stats'] = sdoc['statistics']
    start_doc['save_timestamp'] = time.time()
    # TODO : replace with version lookup in database
    start_doc['analysis_store_version'] = _ANALYSIS_STORE_VERSION

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
    for key, val in sdoc['kwargs'].items():
        if key[0] == '_':
            continue  # ignore hidden keys
        # guess descriptor from data
        descriptor_doc['data_keys'][key] = make_descriptor(val)
        # save to filestore
        if key in external_writers:
            writer_key = external_writers[key]
            if writer_key not in _writers_dict:
                print("Databroker writer : Error, " +
                      "key {} ".format(writer_key) +
                      "not present in writers dict." +
                      " Allowed keys : {}".format(_writers_dict.keys()))
                event_doc['data'][key] = 'Error'
            writer = _writers_dict[writer_key](db.fs)
            # TODO : Move this assumption of file path elsewhere?
            time_now = time.localtime()
            subpath = "/{:04}/{:02}/{:02}".format(time_now.tm_year,
                                                  time_now.tm_mon,
                                                  time_now.tm_mday)
            new_id = writer.write(val, subpath=subpath)
            event_doc['data'][key] = new_id
            descriptor_doc['data_keys'][key].update(external="FILESTORE:")
        else:
            event_doc['data'][key] = safe_parse_databroker(val)
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
