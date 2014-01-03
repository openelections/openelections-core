import os
import sys

from invoke import task

from .utils import load_module

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
})
def run(state):
    """
    Run data validations.

    State is required.
    """
    state_mod = load_module(state, ['validate'])
    for name in dir(state_mod.validate):
        if name.startswith('validate_'):
            func = getattr(state_mod.validate, name)
            print '* %s' % func.func_name
            try:
                func()
            except AssertionError, e:
                sys.exit("Error: %s - %s" % (name, e))
