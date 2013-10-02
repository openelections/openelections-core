from invoke import task, run

from .constants import COUNTRY_DIR
from .cache import list_dir
from .utils import help_text

@task(help=help_text({'cachefile': 'Path to file in state cache directory'}))
def save(state='', datefilter='', cachefile=''):
    """Save files from cache to s3

    Supports saving:

       1) A single file using 'cachefile' argument
       2) All files in cache using 'state' argument, or a
       subset of cached files when 'datefilter' provided.

    """
    # Save individual file and return immediately
    if cachefile:
        print(cached_file)
        pass
    # Save all files for a given state, applying datefilter if present
    if state:
        files = list_dir(state, datefilter)
        if files:
            #TODO: save to S3
            pass
        else:
            msg = "No files found for %s" % state.upper()
            if datefilter:
                msg += " using date filter: %s" % datefilter
            print msg


#TODO
@task(help=help_text({}))
def files(state, datefilter=''):
    """List files on S3 for state

    State required. Optionally provide date filter to 
    limit results.
    """
    pass
