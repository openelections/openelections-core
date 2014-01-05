import re
import string


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
