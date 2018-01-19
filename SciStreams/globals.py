# these are static things to the library
from dask import set_options

from collections import deque

# client information
# TODO : remove this client information
from . import config

MAX_FUTURE_NUM = 1000

if config.server is not None:
    print("Adding a client: {}".format(config.server))
    from distributed import Client
    client = Client(config.server)
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
# allow for 100,000 sinks for these (so we don't lose them)
futures_cache_sinks = deque(maxlen=100000)

# assume all functions are pure globally
try:
    from dask.cache import Cache
    cache = Cache(1e9)
    cache.register()
except ImportError:
    print("Error cachey not available. Will not be caching")
    pass

set_options(delayed_pure=True)

# TAU STUFF Profiler dictionaries
profile_dict = dict()
