# playing around with dask
from dask import compute, delayed
import numpy as np
from functools import wraps
from dask.cache import Cache

_cache = Cache(1e9)
_cache.register()

# data is in
# _cache.cache.data 


@delayed(pure=True)
def circavg(img, calibration):
    return 1

class Foo:
    def __init__(self):
        pass

    @delayed(pure=True)
    def circavg(img, calib):
        return 1

fooclass = Foo()

gg = 10
a = np.ones((gg,gg))
b = 3
res = circavg(a, b)

# keys should not change
print("next")
res_foo = fooclass.circavg(a, b)
print(res._key)
print(res_foo._key)

res_foo.compute()

#res2 = res.compute()
#print(res2)

#res2_foo = res_foo.compute()
#print(res2_foo)

