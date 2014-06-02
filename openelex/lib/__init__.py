def build_github_url(state, generated_filename):
    """
    Generate a URL to a preprocessed result file hosted on GitHub

    Args:
        generated_filename: Standardized filename of an election result file.

    Returns:
        String containing a URL to the preprocessed result file on GitHub.
    
    """
    tpl = "https://raw.githubusercontent.com/openelections/openelections-data-{}/master/{}"
    return tpl.format(state.lower(), generated_filename)


