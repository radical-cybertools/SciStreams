from globals import cache
from dask import delayed

@delayed
def foo(a):
    return a + 1

foo(1).compute()

print(cache.cache.data)
