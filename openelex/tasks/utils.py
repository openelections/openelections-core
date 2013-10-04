def load_module(state, modname):
    return __import__('openelex.us.%s' % state.lower(), fromlist=[modname])

def help_text(extra):
    default = {
        'state':'Two-letter state-abbreviation, e.g. NY',
        'datefilter': 'Any portion of a YYYYMMDD, e.g. 2012, 201211, 20121106',
    }
    default.update(extra)
    return default

HELP_TEXT = help_text({})
