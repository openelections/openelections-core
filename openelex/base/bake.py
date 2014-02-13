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

    def __init__(self, **kwargs):
        """
        Constructor.
        """
        self.kwargs = kwargs
        # TODO: Decide if anything needs to happen here.
        

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

    def build_list(self, **kwargs):
        limiting_kwargs = self.kwargs
        limiting_kwargs.update(kwargs)
        
        filters = self.build_filters(**limiting_kwargs)
        fields = self.build_fields(**limiting_kwargs)
        self._objects = self.get_object_list()
        self._objects = self.apply_filters(self._objects, filters)
        self._objects = self.apply_field_limits(self._objects, fields)

        return self 

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
 
    def build_filters(self, **kwargs):
        """
        Returns a dictionary of filter arguments that will be used to limit
        the queryset.

        This allows for translating arguments from upstream code to the
        filter format used by the underlying data store abstraction.

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
        # TODO: Implement filtering by office, district and party after the 
        # the data is standardized

        # TODO: Sensible defaults for filtering.  By default, should filter to all
        # state/contest-wide results for all races when no filters are specified.
        filters = {
            'state': kwargs.get('state').upper(),
        }

        try:
            filters.update(self.build_date_filters(kwargs.get('datefilter')))
        except AttributeError:
            pass

        return filters

    def build_fields(self, **kwargs):
        """
        Returns a list of fields that will be included in the result or an
        empty list to include all fields.
        """
        return [] 

    def apply_filters(self, objects, filters={}):
        return objects.filter(**filters)
    
    def apply_field_limits(self, objects, fields=[]):
        return objects.only(*fields)

    def get_object_list(self):
        # QUESTION: Is this right, or do we need to merge across datasets?
        # ANSWER: Flatten related models.
        return Result.objects

    def filename(self, **kwargs):
        timestamp = kwargs.get('timestamp', datetime.now())
        fmt = kwargs.get('fmt', 'csv')
        state = self.kwargs.get('state')
        return "%s_%s.%s" % (state,
            timestamp.strftime(self.timestamp_format), fmt) 

    def manifest_filename(self, **kwargs):
        timestamp = kwargs.get('timestamp', datetime.now())
        state = self.kwargs.get('state')
        return "%s_%s_manifest.txt" % (state,
            timestamp.strftime(self.timestamp_format)) 
           
    def write(self, fmt='csv', outputdir=None):
        try:
            fmt_method = getattr(self, 'write_' + fmt) 
        except AttributeError:
            raise UnsupportedFormatError("Format %s is not supported" % (fmt))
        
        if outputdir is None:
            outputdir = self.default_outputdir()

        fmt_method(outputdir)

    def write_csv(self, outputdir):
        raise NotImplementedError

    def write_json(self, outputdir):
        raise NotImplementedError

    def write_manifest(self, outputdir):
        if outputdir is None:
            outputdir = self.default_outputdir()

        raise NotImplementedError
