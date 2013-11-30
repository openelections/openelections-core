import re
import string


def slugify(text, space_sub='_'):
    """Slugify text, with option of character subsitution for whitespace.

    USAGE

        >>> slugify("Testing.  1! 2! 3?")
        'testing_1_2_3'

        >>> slugify("Testing.  1! 2! 3?", space_sub='-')
        'testing-1-2-3'

    """
    text = text.translate(string.maketrans("",""), string.punctuation)
    return re.sub(r'\s+', space_sub, text).lower()
