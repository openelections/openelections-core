import os
import sys

from invoke import task

from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
})
def list(state, group=''):
    """
    Show available transformations on data loaded in MongoDB.

    State is required. Optionally provide 'group' to limit transforms that are performed.
    """
    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.available(state)
    for key, funcs in sorted(transforms.items()):
        print '\n%s:' % key
        for func in funcs:
            print '* %s' % func.func_name


@task(help={
    'state': 'Two-letter state-abbreviation, e.g. NY',
    'group': 'Group of transforms to run (default:all)',
})
def run(state, group='all'):
    """
    Run transformations on data loaded in MongoDB.

    State is required. Optionally provide 'group' to limit transforms that are performed.
    """
    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    try:
        to_run = state_mod.transform.registry.get(state, group)
    except KeyError:
        sys.exit("No transform group '%s' found for '%s'" % (group, state))
    print '\n%s:' % group
    for func in to_run:
        print 'Executing %s transform...' % func.func_name
        func()
