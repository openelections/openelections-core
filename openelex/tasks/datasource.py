from pprint import pprint
import inspect
import os
import sys

from invoke import task

from .utils import load_module


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
    print "\n%s returned %s results" % (func_name, len(results))

HELP = {
    'state':'Two-letter state-abbreviation, e.g. NY',
    'datefilter': 'Any portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.',
}

@task(help=HELP)
def target_urls(state, datefilter=''):
    """
    List source data urls for a state.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    results = handle_task(func_name, state, datefilter)
    pprint_results(func_name, results)

@task(help=HELP)
def mappings(state, datefilter=''):
    """
    List metadata mappings for a state.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    handle_task(func_name, state, datefilter)
    results = handle_task(func_name, state, datefilter)
    pprint_results(func_name, results)

@task(help=HELP)
def elections(state, datefilter=''):
    """
    List elections for a state.

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    results = handle_task(func_name, state, datefilter)
    count = 0
    for year, elecs in results.items():
        count += len(elecs)
        pprint(elecs)
    print "\n%s returned %s results" % (func_name, count)

@task(help=HELP)
def filename_url_pairs(state, datefilter=''):
    """
    List mapping of standard filenames to source urls for a state

    State is required. Optionally provide 'datefilter' to limit  results.
    """
    func_name = inspect.stack()[0][3]
    handle_task(func_name, state, datefilter)
    results = handle_task(func_name, state, datefilter)
    pprint_results(func_name, results)
