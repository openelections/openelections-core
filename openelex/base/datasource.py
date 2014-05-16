from os.path import join, splitext
import re
import urlparse

import unicodecsv

from openelex.api import elections as elec_api
from openelex.lib.text import election_slug, slugify
from .state import StateBase


class BaseDatasource(StateBase):
    """
    Wrapper for interacting with source data.

    Primary use serving as an interface to a state's source data, such as URLs
    of raw data files, and for standardizing names of result files.

    Intended to be subclassed in state-specific datasource.py modules.

    """
    def __init__(self, state=''):
        super(BaseDatasource, self).__init__(state)
        self._cached_url_paths = {}

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

    def jurisdiction_mappings(self, filename=None):
        """Return a list of jurisdictional mappings based on OCD ids"""
        try:
            return self._cached_jurisdiction_mappings
        except AttributeError:
            if filename is None:
                filename = join(self.mappings_dir, self.state + '.csv')

            with open(filename, 'rU') as csvfile:
                reader = unicodecsv.DictReader(csvfile)
                self._cached_jurisdiction_mappings = [row for row in reader]

            return self._cached_jurisdiction_mappings

    def _counties(self):
        try:
            return self._cached_counties
        except AttributeError:
            county_ocd_re = re.compile(r'ocd-division/country:us/state:' +
                    self.state.lower() + r'/county:[^/]+$')
            self._cached_counties = [m for m in self.jurisdiction_mappings()
                if county_ocd_re.match(m['ocd_id'])]
            return self._cached_counties

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

    def _url_paths(self, filename=None):
        """
        Load URL metadata from a CSV file.

        Return a list of dictionaries, with each dict corresponding to a row
        in the CSV file.

        This file should follow the conventsions described at
        http://docs.openelections.net/guide/#populating-urlpathscsv

        Arguments:

        * filename - Path to a URL paths CSV file.  Default is
                     openelex/<state>/mappings/url_paths.csv
        """
        if filename is None:
            filename = join(self.mappings_dir, 'url_paths.csv')

        try:
            # We cache the URL paths to avoid having to do multiple filesystem
            # reads.  We also cache per origin filename to accomodate states
            # like Arkansas where we generate multiple multiple URL path files
            # from scraping
            return self._cached_url_paths[filename]
        except KeyError:
            cached = self._cached_url_paths[filename] = []
            with open(filename, 'rU') as csvfile:
                reader = unicodecsv.DictReader(csvfile)
                for row in reader:
                    cached.append(self._parse_url_path(row))
            return cached 

    def _parse_url_path(self, row):
        """
        Perform data cleaning and type conversion on a URL path file entry.
        """
        clean_row = row.copy()
        # Convert the special flag from string to boolean
        clean_row['special'] = row['special'].lower() == 'true'
        # Add an election_slug entry if it doesn't already exist 
        if 'election_slug' not in clean_row:
            clean_row['election_slug'] = election_slug(self.state,
                clean_row['date'], clean_row['race_type'], clean_row['special']) 
        return clean_row

    def _url_paths_for_election(self, slug, filename=None):
        """Return URL metadata entries for a single election"""
        return [p for p in self._url_paths(filename) if p['election_slug'] == slug]

    def _standardized_filename(self, election, bits=None, **kwargs):
        """
        Standardize a result filename for an election.

        Arguments:

        election - Dictionary containing election metadata as returned by
                   the elections API. Required.
        bits - List of filename elements.  These will be prepended to the
               filename.  List items will be separated by "__".

        Keyword arguments:

        reporting_level
        jurisdiction
        office
        office_district
        extension - Filename extension, including the leading '.'. 
                    Defaults to extension of first file in elections
                    'direct-links'.
        """
        reporting_level = kwargs.get('reporting_level', None)
        jurisdiction = kwargs.get('jurisdiction', None)
        office = kwargs.get('office', None)
        office_district = kwargs.get('office_district', None)
        extension = kwargs.get('extension',
            self._filename_extension(election))

        if bits is None:
            bits = []

        bits.extend([
            election['start_date'].replace('-', ''),
            self.state,
        ])

        if election['special']:
            bits.append('special')

        bits.append(election['race_type'].replace('-', '_'))

        if jurisdiction:
            bits.append(slugify(jurisdiction))

        if office:
            bits.append(slugify(office))

        if office_district:
            bits.append(slugify(office_district))

        if reporting_level:
            bits.append(reporting_level)

        return "__".join(bits) + extension 

    def _filename_extension(self, election):
        parts = urlparse.urlparse(election['direct_links'][0])
        root, ext = splitext(parts.path)
        return ext
