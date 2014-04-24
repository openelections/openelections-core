import sys

from invoke import task

from openelex.base.fetch import BaseFetcher
from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
    'datefilter': 'Any portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.',
    'raw': "Fetch unprocessed data files only.",
})
def fetch(state, datefilter='', raw=False):
    """
    Scrape data files and store in local file cache
    under standardized name.

    State is required. Optionally provide 'datefilter' 
    to limit files that are fetched.
    """
    state_mod = load_module(state, ['datasource', 'fetch'])
    datasrc = state_mod.datasource.Datasource()
    if hasattr(state_mod, 'fetch'):
        fetcher = state_mod.fetch.FetchResults()
    else:
        fetcher = BaseFetcher(state)

    if raw:
        try:
            filename_url_pairs = datasrc.raw_filename_url_pairs(datefilter)
        except NotImplementedError:
            sys.exit("No unprocessed data files are available. Try running this "
                    "task without the --raw option.")
    else:
        filename_url_pairs = datasrc.filename_url_pairs(datefilter)

    for std_filename, url in filename_url_pairs:
        fetcher.fetch(url, std_filename)
