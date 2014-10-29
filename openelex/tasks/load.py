import os.path
import sys

import click

from .utils import default_state_options, load_module

@click.command(name='load.run', help="Load cached data files into the database")
@default_state_options
@click.option('--filename', help="Filename of single file to load")
def run(state, datefilter='', filename=None):
    """
    Load cached data files into MongoDB.

    State is required. Optionally provide 'datefilter' to limit files that are loaded.
    """
    state_mod = load_module(state, ['datasource', 'load'])
    datasrc = state_mod.datasource.Datasource()
    loader = state_mod.load.LoadResults()

    if datefilter and filename:
        sys.stderr.write("You must specify a datefilter or filename but not both")
        sys.exit(1)

    if filename:
        # A filename was specified.  Load only this file.
        mappings = [datasrc.mapping_for_file(os.path.basename(filename))]
    else:
        # Load all files for the specified date filter
        mappings = datasrc.mappings(datefilter)

    #TODO: Notify user if there's a mismatch between expected files and
    # cache.diff
    for mapping in mappings:
        loader.run(mapping)
