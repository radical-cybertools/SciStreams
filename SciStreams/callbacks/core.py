from distributed import Future
from SciStreams.core.StreamDoc import StreamDoc
from SciStreams.globals import client
from SciStreams import globals

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

        remote :
            Force computations to be remote
    '''
    # dictionary of start documents
    def __init__(self, func, *args, remote=True, **kwargs):
        # args and kwargs reserved to forward to the functions
        # print("initiated with kwargs {}".format(kwargs))
        self.args = args
        self.kwargs = kwargs
        self.func = func
        self.remote = remote
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
            print('submitting function {} to server'.format(self.func.__name__))
            res = client.submit(wraps(self.func)(eval_func), self.func, start, descriptor, doc,
                                *self.args, **kwargs)
            if isinstance(res, Future):
                globals.futures_cache_sinks.append(res)
        else:
            # don't do things remotely, so block if things are Futures
            if isinstance(doc, Future):
                doc = doc.result()
            if isinstance(descriptor, Future):
                descriptor = descriptor.result()
            if isinstance(start, Future):
                start = start.result()
            eval_func(self.func, start, descriptor, doc, *self.args,
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
            #raise Exception(msg)
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


def eval_func(func, start, descriptor, event, *args, **kwargs):
    print("on cluster, computing {}".format(func.__name__))
    start_uid = start['uid']
    data = event['data']

    # now make data
    sdoc = StreamDoc()
    sdoc.add(attributes=start)
    # no args since each element in a doc is named
    sdoc.add(kwargs=data)
    checkpoint = dict(parent_uids=[start_uid])
    provenance = dict(name="SciStreamCallback")
    sdoc.add(checkpoint=checkpoint)
    sdoc.add(provenance=provenance)
    # finally, evaluate the function
    # print("calling function {} with sdoc {}".format(func, sdoc))
    # print("calling function {} with extra args : {}".format(func, args))
    # print("extra kwargs : {}".format(kwargs))
    return func(sdoc, *args, **kwargs)
