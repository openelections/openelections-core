from os.path import  join
import json
import urlparse

import unicodecsv

from openelex import PROJECT_ROOT
from openelex.api import elections as elec_api
from openelex.lib.text import election_slug
from .state import StateBase


class BaseDatasource(StateBase):
    """
    Wrapper for interacting with source data.

    Primary use serving as an interface to a state's source data, such as URLs
    of raw data files, and for standardizing names of result files.

    Intended to be subclassed in state-specific datasource.py modules.

    """
    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            # Store elections by year
            self._elections = {}
            for elec in elec_api.find(self.state):
                yr = int(elec['start_date'][:4])
                # Add elec slug
                elec['slug'] = self._election_slug(elec)
                self._elections.setdefault(yr, []).append(elec)
        if year:
            year_int = int(year)
            return {year_int: self._elections[year_int]}
        return self._elections

    def mappings(self, year=None):
        """
        Return an array of dicts, each containing source url and standardized
        filenames for results file, along with other pieces of metadata.
        """
        raise NotImplementedError()

    def target_urls(self, year=None):
        raise NotImplementedError()

    def filename_url_pairs(self, year=None):
        """Return an array of tuples of standardized filename, source url pairs"""
        raise NotImplementedError()

    def unprocessed_filename_url_pairs(self, year=None):
        """
        Return an array of tuples of standardized filename, source URL pairs
        for unprocessed files.
        """
        raise NotImplementedError()

    def standardized_filename(self, url, fname):
        """A standardized, fully qualified path name"""
        #TODO:apply filename standardization logic
        # non-result pages/files use default urllib name conventions
        # result files need standardization logic (TBD)
        if fname:
            filename = join(self.cache_dir, fname)
        else:
            filename = self.filename_from_url(url)
        return filename

    def filename_from_url(self, url):
        #TODO: this is quick and dirty
        # see urlretrieve code for more robust conversion of
        # url to local filepath
        result = urlparse.urlsplit(url)
        bits = [
            self.cache_dir,
            result.netloc + '_' +
            result.path.strip('/'),
        ]
        name = join(*bits)
        return name

    def jurisdiction_mappings(self):
        "Returns a JSON object of jurisdictional mappings based on OCD ids"
        filename = join(PROJECT_ROOT, self.mappings_dir, self.state + '.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)


    def _election_slug(self, election):
        """
        Generate a slug for an election.

        Arguments:

        * election - Dictionary of election attributes as returned by the
                     metadata API.

        """
        # Delete the 'state' key in the election attrs, because its a
        # dict with multiple values we don't care about and we want
        # to just pass the value of self.state to election_slug.  
        # We can probably delete the key from argument without consequence, 
        # but to be safe and avoid side effects,copy the argument first.
        election_attrs = election.copy()
        try:
            del election_attrs['state']
        except  KeyError:
            pass
        return election_slug(self.state, **election_attrs)
