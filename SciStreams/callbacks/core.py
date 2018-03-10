from distributed import Future
from SciStreams.core.StreamDoc import StreamDoc
from SciStreams.config import client
from SciStreams import config

from functools import wraps


class CallbackBase:
    def __call__(self, name, doc):
        "Dispatch to methods expecting particular doc types."
        return getattr(self, name)(doc)

    def event(self, doc):
        pass

    def bulk_events(self, doc):
        pass

    def descriptor(self, doc):
        pass

    def start(self, doc):
        pass

    def stop(self, doc):
        pass


# TODO : This won't work with cloudpickle. Either find out why or just remove
# it. We may want metaclassing though
def FutureCallback(cls):
    class FutureCB(cls):
        def start(self, doc):
            if isinstance(doc, Future):
                client.submit(super().start, doc)
            else:
                super().start(doc)

        def event(self, doc):
            if isinstance(doc, Future):
                client.submit(super().event, doc)
            else:
                super().event(doc)

        def descriptor(self, doc):
            if isinstance(doc, Future):
                client.submit(super().descriptor, doc)
            else:
                super().descriptor(doc)

        def stop(self, doc):
            if isinstance(doc, Future):
                client.submit(super().stop, doc)
            else:
                super().stop(doc)
    return FutureCB


# callback to convert eventstream to a scistream
class SciStreamCallback(CallbackBase):
    ''' This will take a start document and events and output
        each event to a function as a StreamDoc.

        Note: the data elements are saved as doctuples, basically:
                parent_uid, self_uid, document
        This allows for introspecting the tree structure without actually
        calculating the document itself.

        remote :
            Force computations to be remote
    '''
    # dictionary of start documents
    def __init__(self, func, *args, dbname='cms:data', remote=True,
                 fill=False, remote_load=False,
                 **kwargs):
        '''
            remote : bool, optional
                decide whether or not to run on cluster
                    (doesn't work for streams)
            remote_load : bool, optional
                decide whether or not to fill events on cluster or locally
                defaults to true (load on cluster)
            fill : bool, optional
                decide whether or not to fill events
        '''
        # args and kwargs reserved to forward to the functions
        # print("initiated with kwargs {}".format(kwargs))
        self.dbname = dbname
        self.args = args
        self.kwargs = kwargs
        self.func = func
        self.fill = fill
        self.remote = remote
        self.remote_load = remote_load
        self.start_docs = dict()
        self.descriptors = dict()
        # right now init doesn't really do anything
        super(SciStreamCallback, self).__init__()

    def start(self, doctuple):
        # this scheme allows docs to be Futures
        # but still allows us to construct the tree
        # parent_uid, self_uid, doc
        _, start_uid, doc = doctuple
        self.start_docs[start_uid] = (None, doc)

    def descriptor(self, doctuple):
        start_uid, descriptor_uid, doc = doctuple
        # start_uid = doc['run_start']
        # for symmetry, keep the _ and None etc.
        if start_uid not in self.start_docs:
            msg = "Missing start for descriptor"
            msg += "\nDescriptor uid : {}".format(descriptor_uid)
            msg += "\nStart uid: {}".format(start_uid)
            raise Exception(msg)
        # give pointer to start_uid
        # dictionary[self_uid] = (parent_uid, doc)
        self.descriptors[descriptor_uid] = (start_uid, doc)

    def event(self, doctuple):
        dbname = self.dbname
        remote_load = self.remote_load
        # parent_uid, self_uid, doc
        descriptor_uid, event_uid, doc = doctuple
        # descriptor_uid = doc['descriptor']
        if descriptor_uid not in self.descriptors:
            msg = "Missing descriptor for event"
            msg += "\nEvent uid : {}".format(event_uid)
            msg += "\nDescriptor uid: {}".format(descriptor_uid)
            raise Exception(msg)

        start_uid, descriptor = self.descriptors[descriptor_uid]
        _, start = self.start_docs[start_uid]

        kwargs = self.kwargs.copy()
        if self.remote:
            # run but don't return result
            #msg = 'submitting function {} to server'.format(self.func.__name__)
            #print(msg)
            #print("about to submit function {}".format(self.func))
            #print("start : {}".format(start))
            #print("descriptor : {}".format(descriptor))
            #print("doc : {}".format(doc))
            #res = client.submit(wraps(self.func)(eval_func), self.func, start,
            # TODO find out why I can't use "wraps" (scheduler complains about pickling)
            res = client.submit(eval_func, self.func, start, descriptor, doc,
                                *self.args, fill=self.fill,
                                remote_load=remote_load, dbname=dbname,
                                **kwargs)
            # the client may not submit remotely but return a value
            # (depending on the client setup)
            if isinstance(res, Future):
                config.futures_cache_sinks.append(res)
                config.futures_total.inc()
        else:
            # don't do things remotely, so block if things are Futures
            #print("not remote")
            if isinstance(doc, Future):
                doc = doc.result()
            if isinstance(descriptor, Future):
                descriptor = descriptor.result()
            if isinstance(start, Future):
                start = start.result()
            #print("Running function {}".format(self.func))
            eval_func(self.func, start, descriptor, doc, *self.args,
                      fill=self.fill, dbname=dbname, remote_load=remote_load,
                      **kwargs)

    def stop(self, doctuple):
        ''' Stop is where the garbage collection happens.'''
        start_uid, stop_uid, doc = doctuple
        # cleanup the start with start_uid
        self.cleanup_start(start_uid)

    def cleanup_start(self, start_uid):
        if start_uid not in self.start_docs:
            msg = "Warning missing start for stop, skipping"
            print(msg)
            return
            # raise Exception(msg)
        self.start_docs.pop(start_uid)
        desc_uids = list()
        for desc_uid, doctuple in self.descriptors.items():
            desc_start_uid, desc = doctuple
            if desc_start_uid == start_uid:
                desc_uids.append(desc_uid)

        for desc_uid in desc_uids:
            self.cleanup_descriptor(desc_uid)

    def cleanup_descriptor(self, desc_uid):
        if desc_uid in self.descriptors:
            self.descriptors.pop(desc_uid)

from SciStreams.interfaces.databroker.databases import databases
def fill_events(doc, dbname=None):
    ''' fill events in place'''
    # the subset of self.fields that are (1) in the doc and (2) unfilled
    # print("Filled events")
    # print(doc['filled'])

    db = databases[dbname]
    if 'filled' in doc:
        non_filled = [key for key, val in doc['filled'].items() if not val]
        # print("non filled : {}".format(non_filled))
    else:
        non_filled = []

    if len(non_filled) > 0:
        db.fill_event(event=doc, inplace=True)

    return doc


def export_streamdoc(start, descriptor, event, fill=True, seq_num=0,
                     dbname=None):
    ''' turn a set of documents into a stream doc.

        Parameters
        ----------
        start : dict
            start document
        descriptor : dict
            descriptor document
        event : dict
            event document
        fill : bool, optional
            whether or not to fill event
        seq_num : int
            the sequence number

        Returns
        -------
        sdoc : a StreamDoc
        '''
    #print("on cluster, computing {}".format(func.__name__))
    start_uid = start['uid']
    if fill:
        event = fill_events(event, dbname=dbname)
    else:
        # flag unfilled keys
        if 'filled' in event:
            non_filled = [key for key, val in event['filled'].items() if not val]
            # print("non filled : {}".format(non_filled))
        else:
            non_filled = []
        # TODO : make this a descriptor


    data = event['data']
    # now make data
    sdoc = StreamDoc()
    sdoc.add(attributes=start)
    # allow seq_num to be passed
    sdoc['attributes']['seq_num'] = seq_num
    # TODO : make this a descriptor
    sdoc['_unfilled'] = non_filled

    # no args since each element in a doc is named
    sdoc.add(kwargs=data)
    checkpoint = dict(parent_uids=[start_uid])
    provenance = dict(name="SciStreamCallback")
    sdoc.add(checkpoint=checkpoint)
    sdoc.add(provenance=provenance)
    return sdoc


def eval_func(func, start, descriptor, event, *args, dbname=None,
              fill=True, remote_load=False, seq_num=0, **kwargs):
    '''
        fill : bool, optional
            whether to fill events or not
    '''
    #print("on cluster, computing {}".format(func.__name__))
    sdoc = export_streamdoc(start, descriptor, event, dbname=dbname,
                            fill=fill, seq_num=seq_num)
    # finally, evaluate the function
    # print("calling function {} with sdoc {}".format(func, sdoc))
    # print("calling function {} with extra args : {}".format(func, args))
    # print("extra kwargs : {}".format(kwargs))
    return func(sdoc, *args, **kwargs)
