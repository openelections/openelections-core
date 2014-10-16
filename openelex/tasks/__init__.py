from invoke import Collection
from mongoengine import ConnectionError

from openelex.db import init_db
from fetch import fetch
from shell import shell
from publish import publish

import archive, cache, datasource, load, load_metadata, transform, validate, bake

# Build tasks namespace
ns = Collection()
ns.add_collection(archive)
ns.add_collection(bake)
ns.add_collection(cache)
ns.add_collection(datasource)
ns.add_task(fetch)
ns.add_collection(load)
ns.add_collection(load_metadata)
ns.add_task(publish)
ns.add_task(shell)
ns.add_collection(transform)
ns.add_collection(validate)

# Initialize prod Mongo connection
try:
    init_db()
except ConnectionError:
    pass
