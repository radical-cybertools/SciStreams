from dask import set_options
from dask.cache import Cache
from distributed import Client
import cachey

# client information
# TODO : remove this client information
import SciAnalysis.config as config
if config.client is not None:
    client = Client(config.client)
#no client, compute should compute and return nothing
else:
    import dask
    class client:
        def compute(self, *args, **kwargs):
            return dask.compute(*args, **kwargs)[0]


# assume all functions are pure globally
set_options(delayed_pure=True)
cache = Cache(1e9)
cache.register()

tmp_cache = cachey.Cache(1e9)

