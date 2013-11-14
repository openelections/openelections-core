from invoke import task

from openelex.base.cache import StateCache
from .utils import HELP_TEXT, print_files


@task(help=HELP_TEXT)
def files(state, datefilter=''):
    """List files in state cache diretory

    State is required. Optionally provide a date 
    filter to limit results.

    NOTE: Cache must be populated in order to load data.
    """
    cache = StateCache(state)
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
    cache = StateCache(state)
    cache.clear(datefilter)


def cache_discrepancy(self):
    pass
