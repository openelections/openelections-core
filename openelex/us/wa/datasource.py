from os.path import splitext
import urlparse

from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url
from openelex.lib.text import ocd_type_id

class Datasource(BaseDatasource):
    NO_2009_PRIMARY_RESULTS_COUNTIES = [
        'Pierce',
        'Ferry',
        'Wahkiakum',
        'Whatcom',
        'Pend Oreille',
        'Kitsap',
        'Kittitas',
    ]
    """
    Counties that don't have 2009 primary election results.

    See https://github.com/openelections/core/issues/212
    """

    def mappings(self, year=None):
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    def filename_url_pairs(self, year=None):
        return [(mapping['generated_filename'], self._url_for_fetch(mapping)) 
                for mapping in self.mappings(year)]

    def unprocessed_filename_url_pairs(self, year=None):
        return [(mapping['generated_filename'].replace(".csv",
                 self._unprocessed_filename_extension(mapping)),
                 mapping['raw_url'])
                for mapping in self.mappings(year)
                if 'pre_processed_url' in mapping]

    def _unprocessed_filename_extension(self, mapping):
        if mapping['raw_url'].endswith(".pdf"):
            return ".pdf"
        elif mapping['raw_extracted_filename'].endswith(".mdb"):
            return ".mdb"
        else:
            raise Exception

    def _url_for_fetch(self, mapping):
        try:
            return mapping['pre_processed_url']
        except KeyError: 
            return mapping['raw_url']

    def _build_metadata(self, year, elections):
        meta_entries = []

        for election in elections:
            slug = election['slug']
            year = int(election['start_date'].split('-')[0])

            if year <= 2006:
                meta_entries.extend(self._build_metadata_preprocessed(election))
            elif slug == 'wa-2007-08-21-primary':
                meta_entries.extend(self._build_metadata_direct_links(election))
            elif (slug == 'wa-2007-11-06-general' or 
                  (year >= 2008 and year <= 2011)):
                if slug == 'wa-2011-08-16-primary':
                    # The 2011-08-16 election doesn't have any contests of interest for
                    # OpenElections
                    continue

                meta_entries.extend(self._build_metadata_state_county(election))
                meta_entries.extend(self._build_metadata_url_paths(election))

            elif year >= 2012 and year <= 2013:
                meta_entries.extend(self._build_metadata_url_paths(election))
            else:
                msg = ("Not sure how to define mappings for election {}.  "
                       "Please update openelex.us.wa.datasource").format(slug)
                raise NotImplemented(msg)

        return meta_entries

    def _build_metadata_preprocessed(self, election):
        """Return election metadata for an election with preprocessed results"""
        generated_filename = self._standardized_filename(election,
            extension=".csv")
        return [
            {
                'generated_filename': generated_filename,
                'raw_url': build_github_url('wa', generated_filename),
                'ocd_id': 'ocd-division/country:us/state:wa',
                'name': "Washington", 
                'election': election['slug'],
            }
        ]

    def _build_metadata_direct_links(self, election):
        """Return election metadata based on direct_links"""
        meta_entries = []

        for url in election['direct_links']:
            filename_kwargs = {
                'extension': self._filename_extension(url),
            }
            reporting_level = self._reporting_level_from_url(url)
            if reporting_level != 'state':
                filename_kwargs['reporting_level'] = reporting_level
            generated_filename = self._standardized_filename(election,
                **filename_kwargs)
            meta_entries.append({
                'generated_filename': generated_filename,
                'raw_url': url,
                'ocd_id': 'ocd-division/country:us/state:wa',
                'name': "Washington", 
                'election': election['slug'],
            })

        return meta_entries

    def _reporting_level_from_url(self, url):
        parts = urlparse.urlparse(url)
        root, ext = splitext(parts.path)
        root_lower = root.lower()
        if "county" in root_lower:
            return 'county'
        else:
            return 'state'

    def _state_county_csv_results_url(self, election, name):
        url_tpl = "http://vote.wa.gov/results/{}/export/{}_{}.csv"
        date_str = election['start_date'].replace('-', '')
        return url_tpl.format(date_str, date_str, name.replace(' ', ''))

    def _build_metadata_state_county(self, election, extra_statewide=None,
            office=None):
        """
        Generate mappings for the statewide and county CSV files.
        
        This method builds mappings for elections from 2007-2011 that
        have URLs like
        http://vote.wa.gov/results/YYYYMMDD/export/YYYYMMDD_CountyName.csv

        Elections starting in 2012 have very similar results portals.  They
        also provide all county results in a single CSV.  Finally, they
        provide precinct-level CSV data for some counties.  Unfortunately,
        the URLs have a trailing numeric identifier, which doesn't seem to be
        able to be predetermined.  For example the "1451" in
        http://vote.wa.gov/results/20121106/export/20121106_AllCounties_20121205_1451.csv
        
        Just handle these in url_paths.csv.

        Args:
            election: Election dict as returned by the Metadata API.
            extra_statewide: Array of extra names of statewide files.
            office: Office slug if the results are for a single office, e.g. the
                Presidential primary.
        """
        meta_entries = []

        for county in self._counties():
            if (election['slug'] == "wa-2009-08-18-primary" and
                    county['name'] in self.NO_2009_PRIMARY_RESULTS_COUNTIES):
                continue

            generated_filename = self._standardized_filename(election,
                extension=".csv", reporting_level='county',
                jurisdiction=county['name'], office=office)
            meta_entries.append({
                'generated_filename': generated_filename,
                'raw_url': self._state_county_csv_results_url(election, county['name']),
                'ocd_id': county['ocd_id'],
                'name': county['name'],
                'election': election['slug'],
            })

        # There's also a statewide results file that uses the same
        # URL format, but uses "AllState" instead of the county name.  
        # Include it in the mappings also.
        if extra_statewide is None:
            extra_statewide = ["AllState"]
        else:
            extra_statewide.append("AllState")

        for name in extra_statewide:
            filename_kwargs = {
                'extension': ".csv",
                'office': office,
            }
            meta_entries.append({
                'generated_filename': self._standardized_filename(election,
                    **filename_kwargs),
                'raw_url': self._state_county_csv_results_url(election, name),
                'ocd_id': 'ocd-division/country:us/state:wa',
                'name': "Washington", 
                'election': election['slug'],
            })

        return meta_entries

    def _parse_url_path(self, row):
        clean_row = super(Datasource, self)._parse_url_path(row)
        # Convert "TRUE" strings to boolean
        clean_row['skip'] = clean_row['skip'].upper() == "TRUE"
        return clean_row

    def _build_metadata_url_paths(self, election):
        """Return mappings for result files from url_paths.csv"""
        meta_entries = []
        # Exclude paths with the ``skip`` flag set in the mappings
        url_paths = [url_path for url_path in self._url_paths_for_election(election)
                     if not url_path['skip']]

        for url_path in url_paths:
            preprocessed_result = False
            filename_ext = self._filename_extension_for_url_path(url_path)
            # We'll eventually preprocess PDFs and convert them to CSVs.
            # So, the downloaded file will be a CSV.  Set the filename
            # extension accordingly.
            if filename_ext == ".pdf" or filename_ext == ".mdb":
                filename_ext = ".csv"
                preprocessed_result = True

            filename_kwargs = {
                'extension': filename_ext, 
                'reporting_level': url_path['reporting_level'],
                'jurisdiction': url_path['jurisdiction'],
                'party': url_path['party'],
            }
            generated_filename = self._standardized_filename(election,
                **filename_kwargs)

            mapping = {
                'generated_filename': generated_filename,
                'raw_url': url_path['url'], 
                'ocd_id': self._ocd_id_for_url_path(url_path),
                'name': url_path['jurisdiction'],
                'election': election['slug'],
                'raw_extracted_filename': url_path['raw_extracted_filename'],
                'parent_zipfile': url_path['parent_zipfile'],
            }

            if preprocessed_result:
                mapping['pre_processed_url'] = build_github_url(self.state,
                    generated_filename)

            meta_entries.append(mapping)

        return meta_entries

    def _filename_extension_for_url_path(self, url_path):
        # By default, just return an extension from the filename part of the
        # URL
        path = url_path['url'] 
        # But if we have to extract the filename from a zip file, use the
        # extracted filename's extension.
        if url_path['raw_extracted_filename']:
            path = url_path['raw_extracted_filename']
        return self._filename_extension(path)

    def _ocd_id_for_url_path(self, url_path):
        # This method is needed because there can be a url path for either
        # a single, statewide file or a file that contains results for only
        # one county.
        ocd_id = "ocd-division/country:us/state:wa"
        if url_path['jurisdiction']:
            # A jurisdiction is specified, which means that results are
            # broken down per-county
            ocd_id = "{}/county:{}".format(ocd_id, ocd_type_id(url_path['jurisdiction']))
        return ocd_id
