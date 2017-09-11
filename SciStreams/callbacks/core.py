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

