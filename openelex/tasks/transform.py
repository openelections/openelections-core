import sys

from invoke import task

from .utils import load_module, split_args
from validate import run_validation

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
    for transform in transforms:
        print "* %s" % transform
        validators = transform.validators

        if validators:
            print
            print " Validators:"
            for name in validators.keys():
                print "    * %s" % name


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

    # Filter transformations  based in include/exclude flags
    if include:
        to_run = split_args(include)
        for trx in transforms:
            if trx not in to_run:
                transforms.pop(trx)
    if exclude:
        to_skip = split_args(exclude)
        for trx in transforms:
            if trx in to_skip:
                transforms.pop(trx)

    for transform in transforms:
        print 'Executing %s' % transform 
        transform()

        validators = transform.validators
        if validators:
            print "Executing validation"
            run_validation(validators)
