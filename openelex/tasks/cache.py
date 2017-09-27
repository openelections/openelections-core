from __future__ import print_function
import click

from openelex.base.cache import StateCache
from .utils import default_state_options, print_files


@click.command(name="cache.files", help="List files in state cache diretory")
@default_state_options
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
        print(msg) 


@click.command(name='cache.clear', help="Delete files in state cache diretory")
@default_state_options
def clear(state, datefilter=''):
    """Delete files in state cache diretory

    State is required. Optionally provide a date
    filter to limit results.
    """
    cache = StateCache(state)
    cache.clear(datefilter)


def cache_discrepancy(self):
    pass
