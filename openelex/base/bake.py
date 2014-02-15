from bson import json_util
from csv import DictWriter
from datetime import datetime
import json
import os

from openelex import COUNTRY_DIR
from openelex.exceptions import UnsupportedFormatError
from openelex.models import Result, Contest, Candidate

class Roller(object):
    """
    # TODO: Less irreverant docstring
    Because a baker uses a roller to flatten stuff. duh.
    """
    def __init__(self):
        self._results = Result.objects
        self._candidates = Candidate.objects
        self._contests = Contest.objects

    def build_date_filters(self, datestring):
        # QUESTION: Is it better to filter on one of the related fields
        # instead of the denormalized field 
        filters = {}

        # TODO: Better interpretation of date string
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
        filters = {}
        
        filters['state'] = kwargs.get('state').upper()

        try:
            filters['election_id__contains'] = kwargs.get('type')
        except AttributeError:
            pass

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

    def apply_filters(self, filters={}):
        # TODO: Separate filters for each collection
        #self._results.filter(**filters)
        #self._candidates.filter(**filters)
        #self._contests.filter(**filters)
        # Why is this faster than the above?
        self._results = self._results.filter(**filters)
        self._candidates = self._candidates.filter(**filters)
        self._contests = self._contests.filter(**filters)

    def apply_field_limits(self, fields=[]):
        # TODO: Separate fields for each collection
        self._results = self._results.only(*fields)
        self._candidates = self._candidates.only(*fields)
        self._contests = self._contests.only(*fields)

    def flatten(self, result, contest, candidate):
        del result['_id']
        del result['candidate']
        del result['contest']
        contest.pop('_id', None)
        candidate.pop('_id', None)

        flat_contest = { 'contest.' + k: v for (k, v) in contest.items() }
        flat_candidate = { 'candidate.' + k: v for (k, v) in contest.items() }
        result.update(flat_contest)
        result.update(flat_candidate)
        return result 

    def get_list(self, **kwargs):
        filters = self.build_filters(**kwargs)
        fields = self.build_fields(**kwargs)
        self.apply_filters(filters)
        self.apply_field_limits(fields)

        candidate_map = { str(c['_id']):c for c in self._candidates.as_pymongo() }
        contest_map = { str(c['_id']):c for c in self._contests.as_pymongo() }

        self._items = []
        self._fields = set()
        for result in self._results.as_pymongo():
            contest = contest_map[str(result['contest'])]
            candidate = candidate_map[str(result['candidate'])]
            flat = self.flatten(result, contest, candidate)
            self._fields.update(set(flat.keys()))
            self._items.append(flat)

        return self._items

    def get_fields(self):
        return list(self._fields)


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

    def filename(self, fmt, timestamp, **kwargs):
        state = self.kwargs.get('state')
        return "%s_%s.%s" % (state.lower(),
            timestamp.strftime(self.timestamp_format), fmt) 

    def manifest_filename(self, timestamp, **kwargs):
        state = self.kwargs.get('state')
        return "%s_%s_manifest.txt" % (state.lower(),
            timestamp.strftime(self.timestamp_format)) 

    def collect_items(self):
        roller = Roller()
        self._items = roller.get_list(**self.kwargs)
        self._fields = roller.get_fields()
        return self

    def get_items(self):
        return self._items
           
    def write(self, fmt='csv', outputdir=None, timestamp=None):
        """
        Writes data to file.
        
        Arguments:
        
        * fmt: Output format. Either 'csv' or 'json'. Default is 'csv'. 
        * outputdir: Directory where output files will be written. Defaults to 
          "openelections/us/bakery"
          
        """
        try:
            fmt_method = getattr(self, 'write_' + fmt) 
        except AttributeError:
            raise UnsupportedFormatError("Format %s is not supported" % (fmt))
        
        if outputdir is None:
            outputdir = self.default_outputdir()

        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        if timestamp is None:
            timestamp = datetime.now()

        return fmt_method(outputdir, timestamp)

    def write_csv(self, outputdir, timestamp):
        path = os.path.join(outputdir, self.filename('csv', timestamp, **self.kwargs))
        with open(path, 'w') as csvfile:
            writer = DictWriter(csvfile, self._fields)
            writer.writeheader()
            for row in self._items:
                writer.writerow(row)

        return self

    def write_json(self, outputdir, timestamp):
        path = os.path.join(outputdir, self.filename('json', timestamp, **self.kwargs))
        with open(path, 'w') as f:
            f.write(json.dumps(self._items, default=json_util.default))

        return self

    def write_manifest(self, outputdir=None, timestamp=None):
        if outputdir is None:
            outputdir = self.default_outputdir()

        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        if timestamp is None:
            timestamp = datetime.now()

        path = os.path.join(outputdir, self.manifest_filename(timestamp, **self.kwargs))
        with open(path, 'w') as f:
            f.write("TODO: Real manifest output\n")

        return self
