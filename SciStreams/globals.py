from dask import set_options

from collections import deque

MAX_FUTURE_NUM = 1000

# client information
# TODO : remove this client information
from . import config

debugcache = deque(maxlen=1)
if config.client is not None:
    print("Adding a client: {}".format(config.client))
    from distributed import Client
    client = Client(config.client)
# no client, compute should compute and return nothing
else:
    import dask

    class client:
        # make unbound method
        def compute(*args, **kwargs):
            return dask.compute(*args, **kwargs)[0]

futures_cache = deque(maxlen=MAX_FUTURE_NUM)

# assume all functions are pure globally
try:
    from dask.cache import Cache
    cache = Cache(1e9)
    cache.register()
except ImportError:
    print("Error cachey not available. Will not be caching")
    pass

set_options(delayed_pure=True)
