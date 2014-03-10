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
    
    See https://github.com/opencivicdata/ocd-division-ids

    * Valid characters are lowercase UTF-8 letters, numerals (0-9), period (.),
      hyphen (-), underscore (_), and tilde (~).
    * Characters should be converted to UTF-8.
    * Uppercase characters should be converted to lowercase.
    * Spaces should be converted to underscores.
    * All invalid characters should be converted to tildes (~).
    * Leading zeros should be dropped unless doing so changes the meaning of
      the identifier.
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
    # Convert spaces to underscores
    u_text = re.sub(r'\s', u'_', u_text)
    u_text = re.sub(r'[^\w.-~]', u'~', u_text)

    if strip_leading_zeros:
        # Remove leading zeros
        u_text = u_text.lstrip('0')

    return u_text
