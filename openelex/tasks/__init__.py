from invoke import Collection
from mongoengine import ConnectionError

from openelex.settings import init_db
from fetch import fetch

import archive, cache, datasource, load, load_metadata, transform, validate
# TODO: Add bake task back in
# import bake


# Build tasks namespace
ns = Collection()
ns.add_task(fetch)
ns.add_collection(archive)
ns.add_collection(cache)
ns.add_collection(datasource)
ns.add_collection(load)
ns.add_collection(load_metadata)
ns.add_collection(transform)
ns.add_collection(validate)
#ns.add_collection(bake)

# Initialize prod Mongo connection
try:
    init_db()
except ConnectionError:
    pass
