from datetime import datetime
import os

from openelex import COUNTRY_DIR
from openelex.exceptions import UnsupportedFormatError
from openelex.models import Result


class Baker(object):
    """Writes (filtered) election and candidate data to structured files"""

    timestamp_format = "%Y%m%dT%H%M%S"
    """
    stftime() format string used to format timestamps. Mostly used for 
    creating filenames.
    
    Defaults to a version of ISO-8601 without '-' or ':' characters.
    """

    def __init__(self, state, outputdir=None, **kwargs):
        """
        Constructor.

        Arguments:

        * state: Required. Postal code for a state.  For example, "md".
        * outputdir: Directory where output files will be written. Defaults to 
          "openelections/us/bakery"
        * datefilter: Date specified in "YYYY" or "YYYY-MM-DD" used to filter
          elections before they are baked.
        * type: Election type. For example, general, primary, etc. 
        * level: Reporting level of the election results.  For example, "state",
          "county", "precinct", etc. Value must be one of the options specified
          in openelex.models.Result.REPORTING_LEVEL_CHOICES.

        """
        self.state = state.upper()
        # kwargs will mostly be used to construct filters.  Just save them as
        # an attribute so they're available for output filenames.
        self.kwargs = kwargs
        self.outputdir = outputdir if outputdir else self.default_outputdir()

        # TODO: Implement filtering by office, district and party after the 
        # the data is standardized

        # TODO: Sensible defaults for filtering.  By default, should filter to all
        # state/contest-wide results for all races when no filters are specified.

    def default_outputdir(self):
        """
        Returns the default path for storing output files.
       
        This will be used if a directory is not specifically passed to the
        constructor.  It's implemented as a method in case subclasses
        want to base the directory name on instance attributes.
        """
        return os.path.join(COUNTRY_DIR, 'bakery')

    # Abstraction for underlying ORM (in this case MongoEngine).
    # The architecture is lifted largely from Tastypie
    # (https://github.com/toastdriven/django-tastypie/blob/master/tastypie/resources.py)
    # These hooks might be overkill but they help make the code more
    # ORM agnostic and easier to test.

    def get_list(self):
        filters = self.build_filters()
        fields = self.build_fields()
        return self.apply_filters(filters, fields)

    def build_date_filters(self, datestring):
        # QUESTION: Is it better to filter on one of the related fields
        # instead of the denormalized field 
        filters = {}

        if len(datestring) == 4:
            filters['election_id__contains'] = datestring
        elif len(datestring) == 8:
            filters['election_id__contains'] = datetime.strptime(datestring,
                "%Y%m%d").strftime("%Y-%m-%d")

        return filters
 
    def build_filters(self):
        """
        Returns a dictionary of filter arguments that will be used to limit
        the queryset.
        """
        # TODO: Finishing Implementing this
        filters = {
            'state': self.state,
        }

        try:
            filters.update(self.build_date_filters(self.kwargs.get('datefilter')))
        except AttributeError:
            pass

        return filters

    def build_fields(self):
        """
        Returns a list of fields that will be included in the result or an
        empty list.
        """
        return [] 

    def apply_filters(self, filters={}, fields=[]):
        objects = self.get_object_list().filter(**filters).only(*fields)
        return objects

    def get_object_list(self):
        # QUESTION: Is this right, or do we need to merge across datasets?
        return Result.objects

    def filename(self, **kwargs):
        timestamp = kwargs.get('timestamp', datetime.now())
        fmt = kwargs.get('fmt', 'csv')
        return "%s_%s.%s" % (self.state,
            timestamp.strftime(self.timestamp_format), fmt) 

    def manifest_filename(self, **kwargs):
        timestamp = kwargs.get('timestamp', datetime.now())
        return "%s_%s_manifest.txt" % (self.state,
            timestamp.strftime(self.timestamp_format)) 
           
    def write(self, fmt='csv'):
        try:
            fmt_method = getattr(self, 'write_' + fmt) 
        except AttributeError:
            raise UnsupportedFormatError("Format %s is not supported" % (fmt))

        fmt_method()

    def write_csv(self):
        raise NotImplementedError

    def write_json(self):
        raise NotImplementedError

    def write_manifest(self):
        raise NotImplementedError
