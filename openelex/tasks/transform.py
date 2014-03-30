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


class IncludeExcludeError(Exception):
    """
    Error raised when user specifies both an inclusion and exclusion list of
    transforms.
    """
    pass


def _select_transforms(state, include=None, exclude=None):
    """
    Select transforms to run or reverse based on state and a list of transform
    names to include or exclude.
    """
    if include and exclude:
        raise IncludeExcludeError("You can not use both include and exclude flags!")

    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.all(state)
    run_transforms = []

    # Filter transformations based in include/exclude flags
    if include:
        to_run = split_args(include)
        for trx in transforms:
            if trx.name in to_run:
                run_transforms.append(trx)
    if exclude:
        to_skip = split_args(exclude)
        for trx in transforms:
            if trx.name not in to_skip:
                run_transforms.append(trx)

    return run_transforms


@task(help={
    'state': 'Two-letter state-abbreviation, e.g. NY',
    'include': 'Transforms to run (comma-separated list)',
    'exclude': 'Transforms to skip (comma-separated list)',
    'no_reverse': "Don't reverse before running this transform, even if it is set to auto-reverse", 
})
def run(state, include=None, exclude=None, no_reverse=False):
    """
    Run transformations on data loaded in MongoDB.

    State is required. Optionally provide to limit transforms that are performed.
    """
    try:
        run_transforms = _select_transforms(state, include, exclude)
    except IncludeExcludeError as e:
        sys.exit(e)

    for transform in run_transforms:
        if not no_reverse and transform.auto_reverse:
            # Reverse the transform if it's been run previously
            transform.reverse()

        print 'Executing %s' % transform 
        transform()

        validators = transform.validators
        if validators:
            print "Executing validation"
            run_validation(validators)


@task(help={
    'state': 'Two-letter state-abbreviation, e.g. NY',
    'include': 'Transforms to reverse (comma-separated list)',
    'exclude': 'Transforms to skip (comma-separated list)',
})
def reverse(state, include=None, exclude=None):
    """
    Reverse a previously run transformation.

    State is required. Optionally provide to limit transforms that are performed.
    """
    try:
        run_transforms = _select_transforms(state, include, exclude)
    except IncludeExcludeError as e:
        sys.exit(e)

    for transform in run_transforms:
        transform.reverse()
