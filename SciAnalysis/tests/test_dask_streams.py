from streams import Stream

class map(Stream):
    def __init__(self, func, child, **kwargs):
        self.func = func
        self.kwargs = kwargs

        Stream.__init__(self, child)

    def update(self, x, who=None):
        return self.emit(self.func(x, **self.kwargs))


