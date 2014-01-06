import os
import sys

from invoke import task

from .utils import load_module

@task(help={
    'state':'(required) Two-letter state postal, e.g. NY',
})
def list(state):
    """
    Show available transformations on data loaded in MongoDB.

    """
    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.all(state)
    print "\n%s transforms, in order of execution:\n" % state.upper()
    for key, func in transforms.items():
        print '* %s:' % key
    print


@task(help={
    'state': 'Two-letter state-abbreviation, e.g. NY',
    'include': 'Transforms to run (comma-separated list)',
    'exclude': 'Transforms to skip (comma-separated list)',
})
def run(state, include=None, exclude=None):
    """
    Run transformations on data loaded in MongoDB.

    State is required. Optionally provide to limit transforms that are performed.
    """
    if include and exclude:
        sys.exit("ERROR: You can not use both include and exclude flags!")

    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.all(state)

    parse_arg_list = lambda arg_list: set([func_name.strip() for func_name in arg_list.split(',')])

    if include:
        to_run = parse_arg_list(include)
        for trx in transforms:
            if trx not in to_run:
                transforms.pop(trx)
    else:
        to_skip = parse_arg_list(exclude)
        for trx in transforms:
            if trx in to_skip:
                transforms.pop(trx)

    for name, func in transforms.items():
        print 'Executing %s' % name
        func()
