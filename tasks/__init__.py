import os
from os.path import dirname, join

from invoke import run, task

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

PROJECT_ROOT = dirname(dirname(__file__))
PKG_DIR = join(PROJECT_ROOT, 'openelex')
COUNTRY_DIR = join(PKG_DIR, 'us')

def load_module(state, modname):
    return __import__('openelex.us.%s' % state.lower(), fromlist=[modname])

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
    'year': '4-digit election year'
})
def fetch(state, year):
    """
    Scrape raw data files, generate standardized names, 
    and store in local file cache.
    """
    state_mod = load_module(state, 'fetch')
    state_dir = join(COUNTRY_DIR, state) 
    fetcher = state_mod.fetch.FetchResults()
    fetcher.run(year)
