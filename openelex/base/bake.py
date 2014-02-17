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
    Filters and collects related data from document fields into a 
    serializeable format.
    """
    
    datefilter_formats = {
        "%Y": "%Y",
        "%Y%m": "%Y-%m",
        "%Y%m%d": "%Y-%m-%d",
    }
    """
    Map of filter formats as they're specified from calling code, likely
    an invoke task, to how the date should be formatted within a searchable
    data field.
    """

    def __init__(self):
        self._results = Result.objects
        self._candidates = Candidate.objects
        self._contests = Contest.objects

    def build_date_filters(self, datefilter):
        """
        Returns dictionary of appropriate mapper filters based on a date
        string.

        Arguments:

        datefilter: String representation of date.

        """
        filters = {}

        if not datefilter:
            return filters

        # Iterate through the map of supported date formats, try parsing the
        # date filter, and convert it to a mapper filter
        for infmt, outfmt in self.datefilter_formats.items():
            try:
                # For now we filter on the date string in the election IDs
                # under the assumption that this will be faster than filtering
                # across a reference.
                filters['election_id__contains'] = datetime.strptime(
                    datefilter, infmt).strftime(outfmt)
                break
            except ValueError:
                pass
        else:
            raise ValueError("Invalid date format '%s'" % datefilter)
        
        return filters
 
    def build_filters(self, **filter_kwargs):
        """
        Returns a dictionary of filter arguments that will be used to limit
        the mapper queryset.

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
        
        filters['state'] = filter_kwargs.get('state').upper()

        try:
            filters['election_id__contains'] = filter_kwargs.get('type')
        except AttributeError:
            pass

        try:
            filters.update(self.build_date_filters(filter_kwargs.get('datefilter')))
        except AttributeError:
            pass

        return filters

    def build_fields(self, **filter_kwargs):
        """
        Returns a list of fields that will be included in the result or an
        empty list to include all fields.
        """
        return []

    def apply_filters(self, filters={}):
        """
        Filter querysets.
        """
        # Eventually, we might need separate filters for each collection.
        self._results = self._results.filter(**filters)
        self._candidates = self._candidates.filter(**filters)
        self._contests = self._contests.filter(**filters)

    def apply_field_limits(self, fields=[]):
        """
        Limit the fields returned when evaluating the querysets.
        """
        # Eventually, we might need separate field limits for each collection.
        self._results = self._results.only(*fields)
        self._candidates = self._candidates.only(*fields)
        self._contests = self._contests.only(*fields)

    def flatten(self, result, contest, candidate):
        """
        Returns a dictionary representing a single "row" of data, created by
        merging the fields from multiple mapper models/documents.
        """
        # Remove id and reference id fields
        del result['_id']
        del result['candidate']
        del result['contest']
        contest.pop('_id', None)
        candidate.pop('_id', None)

        # Prefix fields on related models for better readability in the final
        # output data.
        flat_contest = { 'contest.' + k: v for (k, v) in contest.items() }
        flat_candidate = { 'candidate.' + k: v for (k, v) in candidate.items() }

        # Merge in the related model. 
        result.update(flat_contest)
        result.update(flat_candidate)

        return result 

    def get_list(self, **filter_kwargs):
        """
        Returns a list of filtered, limited and flattened election results.
        """
        filters = self.build_filters(**filter_kwargs)
        fields = self.build_fields(**filter_kwargs)
        self.apply_filters(filters)
        self.apply_field_limits(fields)

        # It's slow to follow the referenced fields at the MongoEngine level
        # so just build our own map of related items in memory.
        #
        # We use as_pymongo() here, and belowi, because it's silly and expensive
        # to construct a bunch of model instances from the dictionary
        # representation returned by pymongo, only to convert them back to
        # dictionaries for serialization.
        candidate_map = { str(c['_id']):c for c in self._candidates.as_pymongo() }
        contest_map = { str(c['_id']):c for c in self._contests.as_pymongo() }

        # We'll save the flattened items as an attribute to support a 
        # chainable interface.
        self._items = []
        self._fields = set()
        for result in self._results.as_pymongo():
            contest = contest_map[str(result['contest'])]
            candidate = candidate_map[str(result['candidate'])]
            flat = self.flatten(result, contest, candidate)
            # Keep a running list of all the data fields.  We need to do
            # this here because the documents can have dynamic, and therefore
            # differing fields.
            self._fields.update(set(flat.keys()))
            self._items.append(flat)

        return self._items

    def get_fields(self):
        """
        Returns a list of all fields encountered when building the flattened
        data with a call to get_list()

        This list is appropriate for writing a header row in a csv file
        using csv.DictWriter.
        """
        return list(self._fields)


class Baker(object):
    """Writes (filtered) election and candidate data to structured files"""

    timestamp_format = "%Y%m%dT%H%M%S"
    """
    stftime() format string used to format timestamps. Mostly used for 
    creating filenames.
    
    Defaults to a version of ISO-8601 without '-' or ':' characters.
    """

    def __init__(self, **filter_kwargs):
        self.filter_kwargs = filter_kwargs

    def default_outputdir(self):
        """
        Returns the default path for storing output files.
       
        This will be used if a directory is not specifically passed to the
        constructor.  It's implemented as a method in case subclasses
        want to base the directory name on instance attributes.
        """
        return os.path.join(COUNTRY_DIR, 'bakery')

    def filename(self, fmt, timestamp, **filter_kwargs):
        """
        Returns the filename string for the data output file.
        """
        state = self.filter_kwargs.get('state')
        return "%s_%s.%s" % (state.lower(),
            timestamp.strftime(self.timestamp_format), fmt) 

    def manifest_filename(self, timestamp, **filter_kwargs):
        """
        Returns the filename string for the manifest output file.
        """
        state = self.filter_kwargs.get('state')
        return "%s_%s_manifest.txt" % (state.lower(),
            timestamp.strftime(self.timestamp_format)) 

    def collect_items(self):
        """
        Query the data store and store a flattened, filtered list of
        election data.
        """
        roller = Roller()
        self._items = roller.get_list(**self.filter_kwargs)
        self._fields = roller.get_fields()
        return self

    def get_items(self):
        """
        Returns the flattened, filtered list of election data.
        """
        return self._items
           
    def write(self, fmt='csv', outputdir=None, timestamp=None):
        """
        Writes collected data to a file.
        
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
        path = os.path.join(outputdir,
            self.filename('csv', timestamp, **self.filter_kwargs))
            
        with open(path, 'w') as csvfile:
            writer = DictWriter(csvfile, self._fields)
            writer.writeheader()
            for row in self._items:
                writer.writerow(row)

        return self

    def write_json(self, outputdir, timestamp):
        path = os.path.join(outputdir,
            self.filename('json', timestamp, **self.filter_kwargs))
        with open(path, 'w') as f:
            f.write(json.dumps(self._items, default=json_util.default))

        return self

    def write_manifest(self, outputdir=None, timestamp=None):
        """
        Writes a manifest describing collected data to a file.
        """
        if outputdir is None:
            outputdir = self.default_outputdir()

        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        if timestamp is None:
            timestamp = datetime.now()

        path = os.path.join(outputdir,
            self.manifest_filename(timestamp, **self.filter_kwargs))

        # TODO: Decide on best format for manifest file. 
        with open(path, 'w') as f:
            f.write("Generated on %s\n" %
                timestamp.strftime(self.timestamp_format))
            f.write("\n")
            f.write("Filters:\n\n")
            for k, v in self.filter_kwargs.items():
                f.write("%s: %s\n" % (k, v))

        return self
