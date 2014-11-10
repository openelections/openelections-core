import click

def load_module(state, mod_list=[]):
    """Dynamically load modules for states

    USAGE

       mod = load_module(state, ['datasource', 'loader'])
       mod.datasource.Datasource()

    """
    return __import__('openelex.us.%s' % state.lower(), fromlist=mod_list)

def default_state_options(f):
    """Decorator that adds the default options for a state command"""
    d1 = click.option('--state', '-s', required=True,
        help='Two-letter state-abbreviation, e.g. NY')
    d2 = click.option('--datefilter', '-d',
        help='A year specified as YYYY, e.g. 2012', default='')
    return d1(d2(f))


def print_files(files):
    for f in files:
        print f
    print "%s files found" % len(files)

def split_args(raw_args, separator=','):
    """Helper for parsing command-line options"""
    return [func_name.strip() for func_name in raw_args.split(separator)]
