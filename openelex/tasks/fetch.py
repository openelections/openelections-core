import os

from invoke import task

from openelex import COUNTRY_DIR
from openelex.base.fetch import BaseFetcher
from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
    'datefilter': 'Any portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.',
})
def fetch(state, datefilter=''):
    """
    Scrape raw data files and store in local file cache
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

    for std_filename, url in datasrc.filename_url_pairs(datefilter):
        fetcher.fetch(url, std_filename)
