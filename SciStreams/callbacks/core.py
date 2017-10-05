from SciStreams.core.StreamDoc import StreamDoc

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

# callback to convert eventstream to a scistream
class SciStreamCallback(CallbackBase):
    ''' This will take a start document and events and output
        each event to a function as a StreamDoc.
    '''
    # dictionary of start documents
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.start_docs = dict()
        self.descriptors = dict()
        super(SciStreamCallback, self).__init__(*args, **kwargs)

    def start(self, doc):
        self.start_docs[doc['uid']] = doc

    def descriptor(self, doc):
        start_uid = doc['run_start']
        start_docs = self.start_docs
        if start_uid not in start_docs:
            msg = "Missing start for descriptor"
            msg += "\nDescriptor uid : {}".format(doc['uid'])
            msg += "\nStart uid: {}".format(start_uid)
            raise Exception(msg)
        self.descriptors[doc['uid']] = doc

    def event(self, doc):
        descriptor_uid = doc['descriptor']
        if descriptor_uid not in self.descriptors:
            msg = "Missing descriptor for event"
            msg += "\nEvent uid : {}".format(doc['uid'])
            msg += "\nDescriptor uid: {}".format(descriptor_uid)
            raise Exception(msg)

        descriptor = self.descriptors[descriptor_uid]
        start_uid = descriptor['run_start']
        start_doc = self.start_docs[start_uid]

        data = doc['data']

        # now make data
        sdoc = StreamDoc()
        sdoc.add(attributes=start_doc)
        # no args since each element in a doc is named
        sdoc.add(kwargs=data)
        checkpoint = dict(parent_uids=[start_uid])
        provenance = dict(name="SciStreamCallback")
        sdoc.add(checkpoint=checkpoint)
        sdoc.add(provenance=provenance)
        self.func(sdoc)

    def stop(self, doc):
        ''' Stop is where the garbage collection happens.'''
        start_uid = doc['run_start']
        # cleanup the start with start_uid
        self.cleanup_start(start_uid)

    def cleanup_start(self, start_uid):
        if start_uid not in self.start_docs:
            msg = "Error missing start for stop"
            raise Exception(msg)
        self.start_docs.pop(start_uid)
        desc_uids = list()
        for desc_uid, desc in self.descriptors.items():
            if desc['run_start'] == start_uid:
                desc_uids.append(desc_uid)

        for desc_uid in desc_uids:
            self.cleanup_descriptor(desc_uid)

    def cleanup_descriptor(self, desc_uid):
        if desc_uid in self.descriptors:
            self.descriptors.pop(desc_uid)
