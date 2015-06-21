import re


def slugify(text, substitute='_'):
    """Slugify text, with option of character subsitution for whitespace.

    USAGE

        >>> slugify("Testing.  1! 2! 3?")
        'testing_1_2_3'

        >>> slugify("Testing.  1! 2! 3?", substitute='-')
        'testing-1-2-3'

    """
    # remove punctuation and lower case
    text = re.sub(r'[^\w\s]', '', text).lower()
    # Collapse whitespace and convert to substitution string
    return re.sub(r'\s+', ' ', text).replace(' ', substitute)


def ocd_type_id(text, strip_leading_zeros=True):
    """
    Format a string in a way that's suitable for an OCD type ID

    Args:
        text: String to format.
        strip_leading_zeros: Remove leading zeros from name. Default is True.
            For example, '08' would become '8'.

    Returns:
        Formatted string.  See https://github.com/opencivicdata/ocd-division-ids
        for more on the Open Civic Data divsion identifier spec.

        * Valid characters are lowercase UTF-8 letters, numerals (0-9),
          period (.), hyphen (-), underscore (_), and tilde (~).
        * Characters should be converted to UTF-8.
        * Uppercase characters should be converted to lowercase.
        * Spaces should be converted to underscores.
        * All invalid characters should be converted to tildes (~).
        * Leading zeros should be dropped unless doing so changes the meaning
          of the identifier.
    """
    # Use unicode for regexes
    re.UNICODE = True
    # Convert characters to unicode
    try:
        u_text = text.encode('utf-8')
    except AttributeError:
        u_text = unicode(text, "utf-8")
    # Convert to lowercase
    u_text = u_text.lower()
    u_text = u_text.replace('(','')
    u_text = u_text.replace(')','')
    # Convert spaces to underscores
    u_text = re.sub(r'\s', u'_', u_text)
    u_text = re.sub(r'[^\w.\-~]', u'~', u_text)

    if strip_leading_zeros:
        # Remove leading zeros
        u_text = u_text.lstrip('0')

    return u_text


def election_slug(state, start_date, race_type, special=False, **kwargs):
    """
    Generate a standardized election identifier string.

    Args:
        state: Lowercase state postal abbreviation.  For example, "md".
        start_date: Start date of election, in the form YYYY-MM-DD. Required.
        race_type: Race type, for example "general" or "primary".  Required.
        special: Boolean indicating whether the election is a special election.
                Default is False.

    Returns:
        String formatted like: ``{state_abbrev}-YYYY-MM-DD-(special)-{race_type}``

        For example, "ar-2012-05-22-primary".

    """
    bits = [
        state.lower(),
        start_date,
    ]

    if special:
        bits.append('special')

    bits.append(race_type.lower())

    return "-".join(bits)
