from pprint import pprint
import os

from invoke import task

from .constants import COUNTRY_DIR
from .utils import HELP_TEXT

@task(help=HELP_TEXT)
def files(state, datefilter=''):
    """List files in state cache diretory
    
    State is required. Optionally provide a date 
    filter to limit results.

    NOTE: Cache must be populated in order to load data.
    """
    files = list_dir(state, datefilter)
    if files:
        pprint(files)
        print "%s files found" % len(files)
    else:
        msg = "No files found"
        if date_filter:
            msg += " using date filter: %s" % date_filter
        print msg 

def list_dir(state, date_filter=''):
    path_args = (
        COUNTRY_DIR,
        state.lower(), 
        'cache',
    )
    path = os.path.join(*path_args)
    files = os.listdir(path)
    filtered = filter(lambda path: date_filter.strip() in path, files)
    filtered.sort()
    return filtered
