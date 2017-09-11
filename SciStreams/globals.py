# these are static things to the library
from dask import set_options

from collections import deque

# client information
# TODO : remove this client information
from . import config

MAX_FUTURE_NUM = 1000

debugcache = deque(maxlen=1)
if config.client is not None:
    print("Adding a client: {}".format(config.client))
    from distributed import Client
    client = Client(config.client)
# no client, compute should compute and return nothing
else:
    print("No client supported, running locally")

    class Client:
        # make unbound method

        def submit(self, f, *args, **kwargs):
            return f(*args, **kwargs)

        def gather(self, future):
            # it's not a future, just a regular result
            return future

    client = Client()

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
