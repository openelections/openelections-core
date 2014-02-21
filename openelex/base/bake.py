from bson import json_util
from csv import DictWriter
from datetime import datetime
import json
import os

from mongoengine import Q
from mongoengine.fields import ReferenceField

from openelex import COUNTRY_DIR
from openelex.exceptions import UnsupportedFormatError
from openelex.models import Result, Contest, Candidate


class FieldNameTransform(object):
    def __init__(self, doc, field_name, output_name=None): 
        self.collection = doc._meta['collection']
        self.db_field = getattr(doc, field_name).db_field
        self.doc = doc
        self.output_name = output_name


class CalculatedField(object):
    def __init__(self, fn):
        self.fn = fn

    def apply(self, data):
        return self.fn(data)


class RollerMeta(type):
    """
    Metaclass for Roller that allows defining field name transformations
    in a declarative style.
    """
    def __new__(cls, name, bases, attrs):
        field_name_transforms = {}
        field_calculators = {}

        for k, v in attrs.items():
            if isinstance(v, FieldNameTransform):
                transforms = field_name_transforms.setdefault(v.collection, {})
                transforms[v.db_field] = v.output_name if v.output_name else k
                
            elif isinstance(v, CalculatedField):
                field_calculators[k] = v.apply

        attrs['field_name_transforms'] = field_name_transforms
        attrs['field_calculators'] = field_calculators

        return super(RollerMeta, cls).__new__(cls, name, bases, attrs)


class Roller(object):
    """
    Filters and collects related data from document fields into a 
    serializeable format.
    """
    __metaclass__ = RollerMeta
    
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

    collections = [
        Result,
        Candidate,
        Contest
    ]
    """
    List of mapper document/model classes that will be queried and flattened.
    """

    primary_collection = Result
    """
    Mapper document/model class that will "receive" data from other collections.
    """

    # Field name transformations so output fields match the specs at
    # https://github.com/openelections/specs/wiki/Results-Data-Spec-Version-1
    # and 
    # https://github.com/openelections/specs/wiki/Elections-Data-Spec-Version-2 

    # HACK: election_id will get converted to 'id' in the final output.  Had to work around
    # the fact that id is a builtin in Python < 3
    election_id = FieldNameTransform(Result, 'election_id', output_name='id')
    first_name = FieldNameTransform(Candidate, 'given_name')
    last_name = FieldNameTransform(Candidate, 'family_name')
    middle_name = FieldNameTransform(Candidate, 'additional_name')
    name_raw = FieldNameTransform(Candidate, 'raw_full_name')
    votes = FieldNameTransform(Result, 'total_votes')
    division = FieldNameTransform(Result, 'ocd_id')
    updated_at = FieldNameTransform(Contest, 'updated')

    # Calculated fields to match specs.
    #
    # These are run after any of the field name transformations and
    # after data has been merged into a single dictionary from any related
    # documents. So the lambda functions should reference the new name in the
    # dictionary.
    year = CalculatedField(lambda d: d['start_date'].year)

    def __init__(self):
        self._querysets = {}
        self._relationships = {}

        primary_collection_name = self.primary_collection._meta['collection']

        for coll in self.collections:
            name = coll._meta['collection']
            self._querysets[name] = getattr(coll, 'objects')
            if name == primary_collection_name:
                self._primary_queryset = self._querysets[name]

        for field in self.primary_collection._fields.values():
            if self._is_relationship_field(field):
                self._relationships[field.db_field] = field.document_type._meta['collection']

    def _is_relationship_field(self, field):
        return isinstance(field, ReferenceField)

    @property
    def primary_collection_name(self):
        return self.primary_collection._meta['collection']

    def build_date_filters(self, datefilter):
        """
        Returns a query object of filters based on a date string.

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
        
        # Return a Q object rather than just a dict because the non-date
        # filters might also filter with a ``election_id__contains`` keyword
        # argument, clobbering the date filter, or vice-versa
        return Q(**filters)
    
    def build_filters(self, **filter_kwargs):
        """
        Returns a dictionary of Q objects that will be used to limit the 
        mapper querysets.

        This allows for translating arguments from upstream code to the
        filter format used by the underlying data store abstraction.

        This will build a set of filters common to all querysets and will call
        any build_filters_COLLECTION_NAME methods that have been implemented
        on this class.

        Arguments:

        * state: Required. Postal code for a state.  For example, "md".
        * datefilter: Date specified in "YYYY" or "YYYY-MM-DD" used to filter
          elections before they are baked.
        * election_type: Election type. For example, general, primary, etc. 
        * reporting_level: Reporting level of the election results.  For example, "state",
          "county", "precinct", etc. Value must be one of the options specified
          in openelex.models.Result.REPORTING_LEVEL_CHOICES.
          
        """
        # TODO: Implement filtering by office, district and party after the 
        # the data is standardized

        # By default, should filter to all state/contest-wide results for all 
        # races when no filters are specified.
        filters= {}
        q_kwargs = {}
        
        q_kwargs['state'] = filter_kwargs['state'].upper()

        try:
            q_kwargs['election_id__contains'] = filter_kwargs['election_type']
        except KeyError:
            pass

        common_q = Q(**q_kwargs)

        # Merge in the date filters
        try:
            common_q &= self.build_date_filters(filter_kwargs['datefilter'])
        except KeyError:
            pass

        for collection_name in self._querysets.keys():
            filters[collection_name] = common_q
            try:
                fn = getattr(self, 'build_filters_' + collection_name)
                collection_q = fn(**filter_kwargs) 
                if collection_q:
                    filters[collection_name] &= collection_q 
            except AttributeError:
                pass

        return filters

    def build_filters_result(self, **filter_kwargs):
        try:
            return Q(reporting_level=filter_kwargs['reporting_level'])
        except KeyError:
            return None
        
    def apply_filters(self, **queries):
        """
        Filter querysets.
        """
        for collection_name, qs in self._querysets.items():
            q = queries.get(collection_name) 
            if q:
                self._querysets[collection_name] = qs(q)

    def build_fields(self, **filter_kwargs):
        """
        Returns a dictionary where the keys are the collection name and the
        values are lists of fields that will be included in the result or an
        empty list to include all fields.
        """
        return {
            'result': [],
            'candidate': [],
            'contest': [],
        }

    def build_exclude_fields(self, **filter_kwargs):
        return {
            'result': ['candidate_slug', 'contest_slug',],
            'candidate': ['contest', 'contest_slug', 'election_id', 'parties'],
            'contest': ['election_id',],
        }

    def apply_field_limits(self, fields={}, exclude_fields={}):
        """
        Limit the fields returned when evaluating the querysets.
        """
        for collection_name, flds in exclude_fields.items():
            qs = self._querysets[collection_name].exclude(*flds)
            self._querysets[collection_name] = qs

        for collection_name, flds in fields.items():
            qs = self._querysets[collection_name].only(*flds)
            self._querysets[collection_name] = qs

    def transform_field_names(self, data, transforms):
        """Convert field names on a flat row of data"""
        for old_name, new_name in transforms.items():
            try:
                val = data[old_name]
                data[new_name] = val
                del data[old_name]
            except KeyError:
                pass

        return data

    def get_calculated_fields(self, data):
        calculated_fields = {}
        for name, fn in self.field_calculators.items():
            calculated_fields[name] = fn(data)
        return calculated_fields

    def flatten(self, primary, **related):
        """
        Returns a dictionary representing a single "row" of data, created by
        merging the fields from multiple mapper models/documents.
        """
        # Remove id and reference id fields
        primary.pop('_id', None)
        for fname in self._relationships.keys():
            primary.pop(fname, None)
            
        transforms = self.field_name_transforms.get(self.primary_collection_name)
        if transforms:
            primary = self.transform_field_names(primary, transforms)

        # Merge in the related data
        for name, data in related.items():
            # Prefix fields on related models for better readability in the
            # final output data, to prevent clobbering any duplicate keys
            # and to make the fields more accessible to our transformers
            # and calculators.
            data.pop('_id', None)
            transforms = self.field_name_transforms.get(name)
            if transforms:
                data = self.transform_field_names(data, transforms)
            primary.update(data)

        primary.update(self.get_calculated_fields(primary))

        return primary

    def get_list(self, **filter_kwargs):
        """
        Returns a list of filtered, limited and flattened election results.
        """
        filters = self.build_filters(**filter_kwargs)
        fields = self.build_fields(**filter_kwargs)
        exclude_fields = self.build_exclude_fields(**filter_kwargs)
        self.apply_filters(**filters)
        self.apply_field_limits(fields, exclude_fields)

        # It's slow to follow the referenced fields at the MongoEngine level
        # so just build our own map of related items in memory.
        #
        # We use as_pymongo() here, and belowi, because it's silly and expensive
        # to construct a bunch of model instances from the dictionary
        # representation returned by pymongo, only to convert them back to
        # dictionaries for serialization.
        related_map = {}
        for related_field, related_collection in self._relationships.items():
            related_map[related_field] = {
                str(c['_id']):c for c 
                in self._querysets[related_collection].as_pymongo()
            }

        # We'll save the flattened items as an attribute to support a 
        # chainable interface.
        self._items = []
        self._fields = set()
        primary_qs = self._querysets[self.primary_collection_name].as_pymongo()
        for primary in primary_qs:
            related = {}
            for fname, coll in self._relationships.items():
                related[fname] = related_map[coll][str(primary[fname])]
                    
            flat = self.flatten(primary, **related)
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
        return sorted(list(self._fields))


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
