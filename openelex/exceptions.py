class ValidationError(Exception):
    "Raise in models"

class UnsupportedFormatError(Exception):
    """
    Raised when trying to read from or write to an unsupported file format.
    """
    pass
