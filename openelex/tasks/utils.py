def load_module(state, mod_list=[]):
    """Dynamically load modules for states

    USAGE

       mod = load_module(state, ['datasource', 'loader'])
       mod.datasource.Datasource()

    """
    return __import__('openelex.us.%s' % state.lower(), fromlist=mod_list)

def help_text(extra):
    default = {
        'state':'Two-letter state-abbreviation, e.g. NY',
        'datefilter': 'Any portion of a YYYYMMDD, e.g. 2012, 201211, 20121106',
    }
    default.update(extra)
    return default

def print_files(files):
    for f in files:
        print f
    print "%s files found" % len(files)

HELP_TEXT = help_text({})
