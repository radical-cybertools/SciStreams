STORAGEDIR = "/home/lhermitte/SciAnalysis-data"
MASKDIR = STORAGEDIR + "/masks"
XMLDIR = STORAGEDIR + "/xml-files"
# TODO : allow True/False to be possible
DELAYED = True

if DELAYED:
    from dask import delayed
else:
    def delayed(pure=None, pure_default=None):
        def dec(f):
            def fnew(*args, **kwargs):
                return f(*args, **kwargs)
            return fnew
        return dec
