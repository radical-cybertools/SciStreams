from dask import set_options
from dask.cache import Cache

from collections import deque
debugcache = deque(maxlen=1)

# client information
# TODO : remove this client information
import SciAnalysis.config as config
if config.client is not None:
    from distributed import Client
    client = Client(config.client)
#no client, compute should compute and return nothing
else:
    import dask
    class client:
        # make unbound method
        def compute(*args, **kwargs):
            return dask.compute(*args, **kwargs)[0]


# assume all functions are pure globally
try:
    import cachey
    cache = Cache(1e9)
    cache.register()
except ImportError:
    pass

set_options(delayed_pure=True)


