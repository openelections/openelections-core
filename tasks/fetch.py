import os

from invoke import task

from .constants import COUNTRY_DIR
from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
    'datefilter': 'Any portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.',
})
def fetch(state, datefilter=''):
    """
    Scrape raw data files, generate standardized names, 
    and store in local file cache.

    State is required. Optionally provide 'datefilter' 
    to limit files that are fetched.
    """
    state_mod = load_module(state, 'fetch')
    state_dir = os.path.join(COUNTRY_DIR, state) 
    fetcher = state_mod.fetch.FetchResults()
    fetcher.run(datefilter)
