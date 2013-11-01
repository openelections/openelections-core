from invoke import Collection

from fetch import fetch
import archive, cache

# Build tasks namespace
ns = Collection()
ns.add_task(fetch)
ns.add_collection(archive)
ns.add_collection(cache)
