import datetime
import json
from os.path import join
import re

import unicodecsv

from openelex.models import RawResult
from .state import StateBase


class BaseLoader(StateBase):
    """
    Base class for loading results data into MongoDB
    Intended to be subclassed in state-specific load.py modules.
    Reads from cached resources inside each state directory.

    Subclasses should create RawResult models and only do minimal
    cleaning such as:

    * Strip leading/trailing whitespace from values
    * Convert votes from string to integer

    All other cleaning or transformation of values should be
    done by creating transforms.

    """

    def __init__(self):
        super(BaseLoader, self).__init__()

        if not hasattr(self, 'datasource'):
            raise AttributeError("Your loader class must define a datasource attribute")

    def run(self, mapping):
        """
        Load a data file's results into the data store.

        Initializes some metadata attributes on the instance and then
        call ``load()`` to create the RawResult model instances in the
        data store.

        Arguments:

          mapping (dict): A mapping, as returned by Datasource.mappings() that
            includes election metadata, most importantly, a
            ``generated_filename`` value that contains the filename of the
            data to be loaded.

        """
        self.mapping = mapping
        self.source = mapping['generated_filename']
        self.timestamp = datetime.datetime.now()
        self.election_id = mapping['election']

        self.delete_previously_loaded()
        self.load()

    def delete_previously_loaded(self):
        """
        Deletes previously loaded RawResult records for a particular
        data file.
        """
        print("LOAD: %s" % self.source)
        # Reload raw results fresh every time
        result_count = RawResult.objects.filter(source=self.source).count()
        if result_count > 0:
            print("\tDeleting %s previously loaded raw results" % result_count)
            RawResult.objects.filter(source=self.source).delete()

    def load(self):
        """
        Creates records in the data store for each result in the data file.

        This should load the data fields in a way that is as close as possible
        to the original data file.  Only basic data cleaning and transforming
        should be done here, such as stripping leading or trailing whitespace
        or converting number strings to numeric data types.

        This should be implemented in state-specific sublcasses.

        """
        raise NotImplementedError("Your loader class must implement a load method")

    # TODO: Decide if we can remove this.
    def jurisdiction_mappings(self, headers):
        """
        Given a tuple of headers, returns a JSON object of jurisdictional
        mappings based on OCD ids"
        """
        filename = join(self.mappings_dir, self.state+'.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)

    # Private methods

    @property
    def _file_handle(self):
        return open(join(self.cache.abspath, self.source), 'rU')

    @property
    def _xls_file_path(self):
        return join(self.cache.abspath, self.source)

    def _build_common_election_kwargs(self):
        """
        Returns a dictionary of fields derived from the OpenElex API
        and common to all RawResults.

        This dictionary can be used to specify some of the keyword
        arguments when constructing new RawResult records in a
        load implementation.
        """
        year = int(re.search(r'\d{4}', self.election_id).group())
        elecs = self.datasource.elections(year)[year]
        # Get election metadata by matching on election slug
        elec_meta = [e for e in elecs if e['slug'] == self.election_id][0]
        kwargs = {
            'created':  self.timestamp,
            'updated': self.timestamp,
            'source': self.source,
            'election_id': self.election_id,
            'state': self.state.upper(),
            'start_date': datetime.datetime.strptime(elec_meta['start_date'], "%Y-%m-%d"),
            'end_date': datetime.datetime.strptime(elec_meta['end_date'], "%Y-%m-%d"),
            'election_type': elec_meta['race_type'],
            'primary_type': elec_meta['primary_type'],
            'result_type': elec_meta['result_type'],
            'special': elec_meta['special'],
        }
        return kwargs
