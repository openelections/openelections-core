from invoke import Collection
from mongoengine import connect, ConnectionError

from openelex.settings import MONGO
from fetch import fetch
import archive, cache, datasource, load, transform, validate

# Build tasks namespace
ns = Collection()
ns.add_task(fetch)
ns.add_collection(archive)
ns.add_collection(cache)
ns.add_collection(datasource)
ns.add_collection(load)
ns.add_collection(transform)
ns.add_collection(validate)

# Initialize Mongo connection
try:
    connect('openelex', **MONGO['openelex'])
except ConnectionError:
    pass
