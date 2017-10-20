from __future__ import print_function
import csv
from pprint import pprint
import inspect
import sys

import click

from openelex.base.datasource import MAPPING_FIELDNAMES

from .utils import default_state_options, load_module


def handle_task(task, state, datefilter):
    "Call Datasoure methods dynamically based on task function name"
    state_mod_name = "openelex.us.%s" % state
    err_msg = "%s module could not be imported. Does it exist?"
    try:
        state_mod = load_module(state, ['datasource'])
    except ImportError:
        sys.exit(err_msg % state_mod_name)

    try:
        datasrc = state_mod.datasource.Datasource()
    except AttributeError:
        state_mod_name += ".datasource.py"
        sys.exit(err_msg % state_mod_name)
    method = getattr(datasrc, task)
    return method(datefilter)

def pprint_results(func_name, results):
    for result in results:
        pprint(result)
    print("\n%s returned %s results" % (func_name, len(results)))

def csv_results(results):
    fieldname_set = set()
    # Since mappings can have extra fields needed  by a particular state,
    # iterate through the items and record all seen fields.
    for r in results:
        for f in list(r.keys()):
            fieldname_set.add(f)
    # Put known fieldnames first in CSV header output
    fieldnames = [] 
    fieldnames.extend(MAPPING_FIELDNAMES)
    fieldnames.extend(fieldname_set.difference(set(MAPPING_FIELDNAMES)))
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        writer.writerow(r)

@click.command(name='datasource.target_urls', help="List source data urls for a state")
@default_state_options
def target_urls(state, datefilter=''):
    """
    List source data urls for a state.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    results = handle_task(func_name, state, datefilter)
    pprint_results(func_name, results)

@click.command(name='datasource.mappings', help="List metadata mappings for a state")
@default_state_options
def mappings(state, datefilter='', csvout=False):
    """
    List metadata mappings for a state.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    results = handle_task(func_name, state, datefilter)
    if csvout:
        csv_results(results)
    else:
        pprint_results(func_name, results)

@click.command(name='datasource.elections', help="List elections for a state.")
@default_state_options
def elections(state, datefilter=''):
    """
    List elections for a state. This data comes from the OpenElex Metadata API.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    results = handle_task(func_name, state, datefilter)
    count = 0
    for year, elecs in list(results.items()):
        count += len(elecs)
        pprint(elecs)
    print("\n%s returned %s results" % (func_name, count))

@click.command(name='datasource.filename_url_pairs', help="List mapping of standard filenames to source urls for a state")
@default_state_options
def filename_url_pairs(state, datefilter=''):
    """
    List mapping of standard filenames to source urls for a state

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    handle_task(func_name, state, datefilter)
    results = handle_task(func_name, state, datefilter)
    pprint_results(func_name, results)
