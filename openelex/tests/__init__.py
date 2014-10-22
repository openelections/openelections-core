import os
from openelex.base.cache import StateCache

def cache_file_exists(state, filename):
    """
    Does a cache file exist?
    
    This is designed to be a predicate for the skipUnless decorator.
    This allows us to skip tests that depend on a cached downloaded
    results file when that file doesn't exist.

    """
    cache = StateCache(state)
    path = os.path.join(cache.abspath, filename)
    return os.path.exists(path)
