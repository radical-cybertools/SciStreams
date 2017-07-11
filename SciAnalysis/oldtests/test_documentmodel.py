from SciAnalysis.interfaces.streams import Stream
from SciAnalysis.interfaces.databroker.databases import databases
cddb = databases['cms:data']

def latest_start(prevobj, newdoc):
    ''' This accumulator either
            updates the start event if it's a new start event
        or returns an event with the start added.

        It returns a 3 tuple of:
            curstart, curdesc, curevent

        # TODO : this could eventually be a buffer
        # of starts and descriptors with uids
        # when an event comes in, the matching start and desc would be returned
        # if a stop document comes in, then the start and desc are cleared from buffer
        # Note : this requires re-writing scan so that the accumul function returns:
            newstate, result = func(oldstate, incoming)
            as opposed to:
            newstate = func(oldstate, incoming)

            (the latter is more general and way more powerful basically a function
                with its own built in memory)
    '''
    curstart, curdesc, curevent = prevobj

    if newdoc[0] == 'start':
        curstart = 'start', newdoc[1]

    if newdoc[0] == 'descriptor':
        curdesc = 'descriptor', newdoc[1]

    if newdoc[0] == 'event':
        curevent = 'event', newdoc[1]
    else:
        curevent = None, None

    if newdoc[0] == 'stop':
        curstart = 'start', {'uid' : None}
        curdesc = 'descriptor', {'run_start' : {'uid' : None}}
        curevent = 'event', None

    nextobj = curstart, curdesc, curevent

    return nextobj


def valid_event(obj):
    ''' verify uids for the start, desc and event documents.'''
    start, desc, event = obj

    if start[0] != 'start':
        return False
    else:
        startdoc = start[1]
        if 'uid' in startdoc:
            uidstart = start[1]['uid']
        else:
            return False

    if desc[0] != 'descriptor':
        return False
    else:
        descdoc = desc[1]
        if 'run_start' in descdoc:
            uiddesc = descdoc['run_start']['uid']

    if event[0] != 'event':
        return False
    else:
        eventdoc = event[1]
        if eventdoc is not None and 'descriptor' in eventdoc:
            uidevent = eventdoc['descriptor']['run_start']['uid']
        else:
            return False

    return uidstart == uiddesc and uiddesc == uidevent

def get_doc(doclist, docname=None):
    for doc in doclist:
        if doc[0] == docname:
            return doc[1]
    raise ValueError("Error no document found with name {}".format(docname))

def get_img(doc, key=None):
    if 'data' in doc:
        if key in doc['data']:
            return doc['data'][key]
    raise ValueError("Could not find data")

# take in stream of start, descriptor, event, stop documents,
# accumulate for events, and filter for valid accumulated results
s = Stream()
start_def = ('start', {'uid' : None}), ('descriptor',{'run_start' : {'uid' : None}}), (None, None)
# accumulate starts and stops
s_out = s.accumulate(latest_start, start=start_def)
s_out = s_out.filter(valid_event)

s_event = s_out.map(get_doc, docname="event")
s_desc= s_out.map(get_doc, docname="descriptor")
s_start  = s_out.map(get_doc, docname="start")

s_img = s_event.map(get_img, key="pilatus300_image")

s_out.map(lambda x : print("got valid event"))



from collections import deque
imglist = deque(maxlen=10)
s_img.map(imglist.append)


hdr = cddb(start_time = "2017-06-08 9:00")
docgen = cddb.restream(hdr,fill=True)

for doc in docgen:
    s.emit(doc)
