from __future__ import print_function
from __future__ import absolute_import
import sys

import click

from .validate import run_validation

from .utils import load_module, split_args

@click.command(name='transform.list', help="Show available data transformations")
@click.option('--state', required=True, help="Two-letter state postal, e.g. NY")
@click.option('--raw', is_flag=True, help="List raw transforms")
def list(state, raw=False):
    """
    Show available transformations on data loaded in MongoDB.

    """
    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.all(state)
    print("\n%s transforms, in order of execution:\n" % state.upper())
    for transform in transforms:
        print("* %s" % transform)
        validators = transform.validators

        if validators:
            print()
            print(" Validators:")
            for name in list(validators.keys()):
                print("    * %s" % name)


class IncludeExcludeError(Exception):
    """
    Error raised when user specifies both an inclusion and exclusion list of
    transforms.
    """
    pass


def _select_transforms(state, include=None, exclude=None, raw=False):
    """
    Select transforms to run or reverse based on state and a list of transform
    names to include or exclude.
    """
    if include and exclude:
        raise IncludeExcludeError("You can not use both include and exclude flags!")

    # Iniitialize transforms for the state in global registry
    state_mod = load_module(state, ['transform'])
    transforms = state_mod.transform.registry.all(state, raw=raw)
    run_transforms = []

    # Filter transformations based in include/exclude flags
    if include:
        to_run = split_args(include)
        for trx in transforms:
            if trx.name in to_run:
                run_transforms.append(trx)
    elif exclude:
        to_skip = split_args(exclude)
        for trx in transforms:
            if trx.name not in to_skip:
                run_transforms.append(trx)
    else:
        run_transforms = transforms

    return run_transforms


@click.command(name='transform.run', help="Run data transformations")
@click.option('--state', required=True, help="Two-letter state-abbreviation, e.g. NY")
@click.option('--include', help="Transforms to run (comma-separated list)")
@click.option('--exclude', help="Transforms to skip (comma-separated list)")
@click.option('--no-reverse', is_flag=True, help="Don't reverse before running this "
    "transform, even if it is set to auto-reverse")
@click.option('--raw', is_flag=True, help="Transforms to run are raw transforms")
def run(state, include=None, exclude=None, no_reverse=False, raw=False):
    """
    Run transformations on data loaded in MongoDB.

    State is required. Optionally provide to limit transforms that are performed.
    """
    try:
        run_transforms = _select_transforms(state, include, exclude, raw)
    except IncludeExcludeError as e:
        sys.exit(e)

    for transform in run_transforms:
        if not no_reverse and transform.auto_reverse:
            # Reverse the transform if it's been run previously
            transform.reverse()

        print('Executing %s' % transform) 
        transform()

        validators = list(transform.validators.values())
        if validators:
            print("Executing validation")
            run_validation(state, validators)

@click.command(name='transform.reverse', help="Reverse a previously run transformation")
@click.option('--state', required=True, help="Two-letter state-abbreviation, e.g. NY")
@click.option('--include', help="Transforms to reverse (comma-separated list)")
@click.option('--exclude', help="Transforms to skip (comma-separated list)")
@click.option('--raw', is_flag=True, help="Transforms to reverse are raw transforms")
def reverse(state, include=None, exclude=None, raw=False):
    """
    Reverse a previously run transformation.

    State is required. Optionally provide to limit transforms that are performed.
    """
    try:
        run_transforms = _select_transforms(state, include, exclude, raw)
    except IncludeExcludeError as e:
        sys.exit(e)

    for transform in run_transforms:
        transform.reverse()
