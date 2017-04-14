# TODO : have config read a yaml file or local file
# TODO : Also add a reload routine
STORAGEDIR = "/home/lhermitte/SciAnalysis-data"
MASKDIR = STORAGEDIR + "/masks"

RESULTSROOT = "/home/lhermitte/sqlite"
XMLDIR = RESULTSROOT + "/xml-files"
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
