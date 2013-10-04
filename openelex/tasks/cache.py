import os

from invoke import task

from openelex import COUNTRY_DIR
from .utils import HELP_TEXT, print_files

@task(help=HELP_TEXT)
def files(state, datefilter=''):
    """List files in state cache diretory
    
    State is required. Optionally provide a date 
    filter to limit results.

    NOTE: Cache must be populated in order to load data.
    """
    cache = StateCache(COUNTRY_DIR, state)
    files = cache.list_dir(datefilter)
    if files:
        print_files(files)
    else:
        msg = "No files found"
        if datefilter:
            msg += " using date filter: %s" % datefilter
        print msg 

@task(help=HELP_TEXT)
def clear(state, datefilter=''):
    """Delete files in state cache diretory

    State is required. Optionally provide a date
    filter to limit results.
    """
    cache = StateCache(COUNTRY_DIR, state)
    cache.clear(datefilter)

class StateCache(object):

    def __init__(self, country_dir, state):
        self.country_dir = country_dir
        self.state = state.lower()
        self.path = os.path.join(country_dir, self.state, 'cache')

    def list_dir(self, datefilter=''):
        files = os.listdir(self.path)
        filtered = filter(lambda path: datefilter.strip() in path, files)
        filtered.sort()
        return filtered

    def clear(self, datefilter=''):
        files = self.list_dir(datefilter)
        [os.remove(os.path.join(self.path, f)) for f in files]
        remaining = self.list_dir()
        print "%s files deleted" % len(files)
        print "%s files still in cache" % len(remaining)
