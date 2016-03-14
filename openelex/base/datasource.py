from os.path import join, splitext
import re
import urlparse

import unicodecsv

from openelex.api import elections as elec_api
from openelex.lib.text import election_slug, slugify
from .state import StateBase


MAPPING_FIELDNAMES = [
    'election',
    'raw_url',
    'generated_filename',
    'pre_processed_url',
    'ocd_id',
    'name',
]
"""Base fields in mapping dictionaries"""

class BaseDatasource(StateBase):
    """
    Wrapper for interacting with source data.

    Its primary use is serving as an interface to a state's source data,
    such as URLs of raw data files, and for standardizing names of result
    files.

    It should be subclassed in state-specific modules as
    ``openelex.us.{state_abbrev}.datasource.Datasource``.

    """
    def __init__(self, state=''):
        super(BaseDatasource, self).__init__(state)
        self._cached_url_paths = {}

    def elections(self, year=None):
        """
        Retrieve election metadata for this state.

        Args:
            year: Only return metadata for elections from the specified year,
            provided as an integer.  Defaults to returning elections
            for all years.

        Returns:
            A dictionary, keyed by year.  Each value is a list of dictonariess,
            each representing an election and its metadata for that year.

            The election dictionaries match the output of the
            Metadata API (http://docs.openelections.net/metadata-api/).

            The election dictionaries have an additional ``slug`` key that
            can be used as an election identifier.

        """
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
        Retrieve source URL, standardized filename and other metadata for a results file.

        This must be implemented in the state-specific class.

        Args:
            year: Only return mappings for elections from the specified year,
                provided as an integer.  Defaults to returning mappings for
                all elections.

        Returns:
            A list of dicts, each containing source URL and standardized
            filenames for a results file, along with other pieces of metadata.

            For a state with one results file per election, the returned list
            will contain only a single mapping dictionary.  For states that split results
            across multiple data files by county, congressional district or
            precinct, there will be one dictionary per result file.

            The return dictionary should include the following keys:

            * **ocd_id**: The Open Civic Data identifier for the jurisdiction
                that the data file covers.
            * **election**: An identifier string for the election.  You
                should use the ``slug`` value from the dictionaries returned by
                the ``elections()`` method.
            * **raw_url**: The full URL to the raw data file.
            * **generated_filename**: The standardized filename that will be
                used for the locally cached copy of the data.  For more on
                filename standardization, see
                http://docs.openelections.net/archive-standardization/
            * **name**: The name of the jurisdiction that the data file covers.

            **Example mapping dictionary:**

            ```
            {
              "ocd_id": "ocd-division/country:us/state:md/place:baltimore",
              "election": "md-2012-04-03-primary",
              "raw_url": "http://www.elections.state.md.us/elections/2012/election_data/Baltimore_City_By_Precinct_Republican_2012_Primary.csv",
              "generated_name": "20120403__md__republican__primary__baltimore_city__precinct.csv",
              "name": "Baltimore City"
            }
            ```

            The ``ocd_id`` and ``name`` values in the returned dictionary
            should reflect the jurisdiction that the data covers.  For
            example, a file containing results for all of Delaware would have
            an ocd_id of ``ocd-division/country:us/state:de`` and a name of
            "Delaware".

            A file containing results for Worcester County, Maryland would
            have an ocd_id of
            ``ocd-division/country:us/state:md/county:worcester`` and a name
            of "Worcester".

        """
        raise NotImplementedError()

    def mapping_for_file(self, filename):
        """Get the mapping for a generated filename"""
        year = self._filename_year(filename)
        try:
            return next(m for m in self.mappings(year)
                        if m['generated_filename'] == filename)
        except StopIteration:
            msg = "Mapping for standardized filename {} could not be found"
            msg = msg.format(filename)
            raise LookupError(msg)

    @classmethod
    def _filename_year(cls, filename):
        """Extract the year of the election from a standardized filename"""
        return filename[0:4]

    def target_urls(self, year=None):
        """
        Retrieve source data URLs.

        This must be implemented in the state-specific class.

        Args:
            year: Only return URLs for elections from the specified year,
                provided as an integer.  Default is to return URLs for all
                elections.

        Returns:
             A list of source data URLs.

        """
        raise NotImplementedError()

    def filename_url_pairs(self, year=None):
        """
        Retrieve standardized filename, source url pairs.

        This must be implemented in the state-specific class.

        Args:
            year: Only return URLs for elections from the specified year.
                 Default is to return URL and filename pairs for all elections.

        Returns:
            A list of standardized filename, source url pairs.

        """
        raise NotImplementedError()

    def unprocessed_filename_url_pairs(self, year=None):
        """
        Retrieve standardized filename, source URL pairs for unprocessed files.

        This should be implemented in some state-specific classes.

        It is only needed for states where we have to preprocess the raw data
        from the state, for example, to convert PDFs to CSV.  In most cases,
        you won't have to implement this.

        Args:
            year: Only return URLs for elections from the specified year.
                 Default is to return URL and filename pairs for all elections.

        Returns:
            An array of tuples of standardized filename, source URL pairs.

        """
        return []

    def jurisdiction_mappings(self, filename=None):
        """
        Retrieve jurisdictional mappings based on OCD IDs.

        Args:
            filename: Filename of the CSV file containing jurisdictional
            mappings.  Default is
            openelex/us/{state_abbrev}/mappings/{state_abbrev}.csv.

        Returns:
            A list of dictionaries containing jurisdiction Open Civic Data
            identifiers, jurisdiction names and other metadata about
            the jurisdiction.  The return dictionaries include a
            value for each column in the input CSV.

            Example jurisdiction mapping dictionary:

            ```
            {
                'ocd_id': 'ocd-division/country:us/state:ar/county:washington',
                'fips': '05143',
                'name': 'Washington'
            }
            ```

        """
        try:
            return self._cached_jurisdiction_mappings
        except AttributeError:
            if filename is None:
                filename = join(self.mappings_dir, self.state + '.csv')

            with open(filename, 'rU') as csvfile:
                reader = unicodecsv.DictReader(csvfile)
                self._cached_jurisdiction_mappings = [row for row in reader]

            return self._cached_jurisdiction_mappings

    def place_mappings(self, filename=None):
        try:
            return self._cached_place_mappings
        except AttributeError:
            if filename is None:
                filename = join(self.mappings_dir, self.state + '_places.csv')

            with open(filename, 'rU') as csvfile:
                reader = unicodecsv.DictReader(csvfile)
                self._cached_place_mappings = [row for row in reader]

            return self._cached_place_mappings

    def _counties(self):
        """
        Retrieve jurisdictional mappings for a state's counties.

        Returns:
            A list of dictionaries containing jurisdiction metadata, as
            returned by ``jurisdictional_mappings()``.

        """
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

        Args:
            election: Dictionary of election attributes as returned by the
                 metadata API.

        Returns:
            A string containing a unique identifier for an election.  For
            example, "ar-2012-05-22-primary".

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

        The CSV file should follow the conventsions described at
        http://docs.openelections.net/guide/#populating-urlpathscsv

        Args:
            filename: Path to a URL paths CSV file.  Default is
                 openelex/{state_abbrev}/mappings/url_paths.csv

        Returns:
            A list of dictionaries, with each dict corresponding to a row
            in the CSV file.

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

        Args:
            row: Dictionary representing a single row of data in the CSV file.

        Returns:
            A dictionary with cleaned and transformed values.  The
            ``special`` value is converted from a string to a boolean
            and an ``election_slug`` value is added if not already
            present.

        """
        clean_row = row.copy()
        # Convert the special flag from string to boolean
        clean_row['special'] = row['special'].lower() == 'true'
        # Add an election_slug entry if it doesn't already exist
        if 'election_slug' not in clean_row:
            clean_row['election_slug'] = election_slug(self.state,
                clean_row['date'], clean_row['race_type'], clean_row['special'])
        return clean_row

    def _url_paths_for_election(self, election, filename=None):
        """
        Retrieve URL metadata entries for a single election.

        Args:
            election: Election metadata dictionary as returned by the
            elections() method or string containing an election slug.

        Returns:
            A list of dictionaries, like the return value of
            ``_url_paths()``.

        """
        try:
            slug = election['slug']
        except TypeError:
            slug = election

        return [p for p in self._url_paths(filename) if p['election_slug'] == slug]

    def _standardized_filename(self, election, bits=None, **kwargs):
        """
        Standardize a result filename for an election.

        For more on filename standardization conventsions, see
        http://docs.openelections.net/archive-standardization/.

        Args:
            election: Dictionary containing election metadata as returned by
            the elections API. Required.
            bits: List of filename elements.  These will be prepended to the
                filename.  List items will be separated by "__".
            reporting_level: Slug string representing the reporting level of
                the data file.  This could be something like 'county' or
                'precinct'.
            jurisdiction: String representing the jurisdiction of the data
                covered in the file.
            office: String representing the office if results are for a single
                office.
            office_district: String representing the office district numver if
               the results in the file are for a single office.
            extension: Filename extension, including the leading '.'.
                  Defaults to extension of first file in election's
                  ``direct_links``.

        Returns:
            A string representing the standardized filename for
            an election's data file.

        """
        # TODO(geoffhing@gmail.com) Delegate to
        # openelex.lib.standardized_filename()
        reporting_level = kwargs.get('reporting_level')
        jurisdiction = kwargs.get('jurisdiction')
        office = kwargs.get('office')
        office_district = kwargs.get('office_district')
        extension = kwargs.get('extension')
        if extension is None:
            extension =  self._filename_extension(election['direct_links'][0])

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

    def _filename_extension(self, url):
        parts = urlparse.urlparse(url)
        root, ext = splitext(parts.path)
        return ext
