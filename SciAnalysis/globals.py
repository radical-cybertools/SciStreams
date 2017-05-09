from dask import set_options
from dask.cache import Cache
from distributed import Client
import cachey

# client information
if True:
    client = Client("10.11.128.3:8786")
#no client, compute should compute and return nothing
else:
    import dask
    class client:
        def compute(self, *args, **kwargs):
            dask.compute(*args, **kwargs)
            return None


# assume all functions are pure globally
set_options(delayed_pure=True)
cache = Cache(1e9)
cache.register()

tmp_cache = cachey.Cache(1e9)

