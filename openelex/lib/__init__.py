from datetime import datetime
import functools

from openelex.lib.text import slugify

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

def build_raw_github_url(state, datestring, raw_filename):
    """
    Generate a URL to a raw result file hosted on GitHub

    Args:
        raw_filename: Raw filename of an election result file.

    Returns:
        String containing a URL to the raw result file on GitHub.

    """
    tpl = "https://raw.githubusercontent.com/openelections/openelections-data-{}/master/{}/{}"
    return tpl.format(state.lower(), datestring, raw_filename)

def standardized_filename(state, start_date, extension,
    party=None, special=False, race_type=None, reporting_level=None,
    jurisdiction=None, office=None, office_district=None,
    prefix_bits=[], suffix_bits=[], sep="__"):
    """
    Standardize an election-related filename.

    For more on filename standardization conventions, see
    http://docs.openelections.net/archive-standardization/.

    Args:
        state (string): State abbreviation, for example "md".
        start_date (string): String representing the election's start date
            in YYYY-MM-DD format.
        extension (string): Filename extension, including the leading '.'.  For
            example, ".csv".
        party (string, optional): Slug representing the political party of
            information in the file.
        special (boolean, optional): Whether the file contains data related to a
            special election.  Default is False.
        race_type (string, optional): Slug representing the type of election
            contest.  For example, "general", "primary", etc.
        reporting_level (string, optional): Slug representing the reporting level of
            the data file.  This could be something like 'county' or
            'precinct'.
        jurisdiction (string, optional): The jurisdiction of the data
            covered in the file.
        office (string, optional): Office if results are for a single office.
        office_district (string, optional): Office district number if
           the data in the file are for a single office.
        prefix_bits (list, optional): List of strings that will be prepended to
            the generated filename and separated by the value of ``sep``.
        suffix_bits (list, optional): List of strings that will be appended
            to the generated filename and separated by the value of ``sep``.
        sep (string, optional): Separator between filename elements.  Default
            is "__".

    Returns:
        A string representing the standardized filename for
        election-related data.

    """
    # Store filename components in a list that we'll eventually join together
    bits = []

    # Filename starts with the prefix bits, if any
    bits.extend(prefix_bits)

    # All filenames need a date and a state
    bits.extend([
        start_date.replace('-', ''),
        state.lower(),
    ])

    if special:
        bits.append('special')

    if party:
        bits.append(slugify(party))

    if race_type:
        bits.append(race_type.replace('-', '_'))

    if jurisdiction:
        bits.append(slugify(jurisdiction))

    if office:
        bits.append(slugify(office))

    if office_district:
        bits.append(slugify(office_district))

    if reporting_level:
        bits.append(reporting_level)

    bits.extend(suffix_bits)

    return sep.join(bits) + extension


def format_date(datestr):
    """
    Convert date string into a format used within a searchable data field.

    This is needed because calling code, likely an invoke task uses dates
    in "%Y%m%d" format and the data store uses dates in "%Y-%m-%d" format.

    Args:
        datestr (string): Date string in "%Y%m%d" format.

    Returns:
        Date string in "%Y-%m-%d" format.

    Raises:
        ValueError if date string is not in an expected format.

    """
    datefilter_formats = {
        "%Y": "%Y",
        "%Y%m": "%Y-%m",
        "%Y%m%d": "%Y-%m-%d",
    }

    for infmt, outfmt in datefilter_formats.items():
        try:
            return datetime.strptime(datestr, infmt).strftime(outfmt)
        except ValueError:
            pass
    else:
        raise ValueError("Invalid date format '{}'".format(datestr))


def compose(*functions):
    """
    Compose an arbitary number of functions

    Implementation by Mathieu Larose
    https://mathieularose.com/function-composition-in-python

    """
    def compose2(f, g):
        return lambda x: f(g(x))
    return functools.reduce(compose2, functions)
