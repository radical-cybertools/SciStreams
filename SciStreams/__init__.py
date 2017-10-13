# this reads from .config and gets config info automatically
from .interfaces.databroker.databases import databases  # noqa
# cadb = databases['cms:analysis']
# cddb = databases['cms:data']
from .core.StreamDoc import StreamDoc  # noqa
