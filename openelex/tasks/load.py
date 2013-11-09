import os

from invoke import task

from openelex.base.load import BaseLoader
from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
    'datefilter': 'Any portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.',
})
def run(state, datefilter=''):
    """
    Load cached data files into MongoDB.

    State is required. Optionally provide 'datefilter' to limit files that are loaded.
    """
    state_mod = load_module(state, ['datasource', 'load'])
    datasrc = state_mod.datasource.Datasource()
    loader = state_mod.load.LoadResults()

    #TODO: Notify user if there's a mismatch between expected files and cache.diff
    for mapping in datasrc.mappings(datefilter):
        loader.run(mapping)
