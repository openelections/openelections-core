def build_github_url(state, generated_filename):
    """Return a URL to the preprocessed files hosted on GitHub"""
    tpl = "https://raw.githubusercontent.com/openelections/openelections-data-{}/master/{}"
    return tpl.format(state.lower(), generated_filename)


