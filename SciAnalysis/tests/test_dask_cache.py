from dask.cache import Cache
from dask import delayed

my_cache = Cache(1e9)
my_cache.register()


from SciAnalysis.interfaces.SciResult import parse_sciresults, SciResult

a = SciResult(a=10)

@delayed(pure=True)
@parse_sciresults("myfunc")
def myfunc(a, b, c):
    return a + b + c


res = myfunc(a,2,3)
