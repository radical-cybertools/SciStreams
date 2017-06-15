# this reads from .config and gets config info automatically
from .interfaces.databroker.databases import databases
cadb = databases['cms:analysis']
cddb = databases['cms:analysis']
