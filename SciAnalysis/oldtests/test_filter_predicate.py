class NOOP:
    pass

from SciAnalysis.interfaces.streams import stream_map
@stream_map.register(NOOP)
def _(*args, **kwargs):
    print("noop")
    return NOOP()

