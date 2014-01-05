import os
import sys

from invoke import task

from .utils import load_module


@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
})
def list(state):
    state_mod = load_module(state, ['validate'])
    print "\nAvailable validators:\n"
    for name in dir(state_mod.validate):
        if name.startswith('validate_'):
            func = getattr(state_mod.validate, name)
            out = "\t%s" % name
            if func.func_doc:
                out += "\n\t\t %s" % func.func_doc
            print out + "\n"

@task(help={
    'state':'Two-letter state-abbreviation, e.g. NY',
})
def run(state):
    """
    Run data validations.

    State is required.
    """
    state_mod = load_module(state, ['validate'])
    passed = []
    failed = []
    print
    for name in dir(state_mod.validate):
        if name.startswith('validate_'):
            func = getattr(state_mod.validate, name)
            try:
                func()
                passed.append(name)
            except Exception as e:
                failed.append("Error: %s - %s - %s" % (state.upper(), name, e))

    print "\n\nVALIDATION RESULTS"
    print "Passed: %s" % len(passed)
    print "Failed: %s" % len(failed)
    for fail in failed:
        print "\t%s" % fail
    print

