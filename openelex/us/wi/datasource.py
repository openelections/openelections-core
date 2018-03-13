"""
Wisconsin has Excel files containing precinct(ward)-level results for elections back to 2002.
All of the files are pre-processed into CSV and available on GitHub at
https://github.com/openelections/openelections-data-wi.

There is one file per election, including national and state offices.
The CSV files are named according to whether they're general/primary, and whether they're special.
"""
from future import standard_library
standard_library.install_aliases()
from os.path import join
import json
import datetime
import urllib.parse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url, build_raw_github_url

class Datasource(BaseDatasource):

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """
        Return array of dicts containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in list(self.elections(year).items()):
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        "Get list of source data urls, optionally filtered by year"
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], self._url_for_fetch(item))
                for item in self.mappings(year)]

    def _url_for_fetch(self, item):
        try:
            return item['pre_processed_url']
        except KeyError:
            return item['raw_url']

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            try:
                raw_url = election['direct_links'][0] # In reality, the election may have multiple source files, but we shouldn't be using the raw_url for anything
            except IndexError:
                raw_url = election['direct_link']
            generated_filename = self._generate_filename(election)
            ocd_id = 'ocd-division/country:us/state:wi'
            name = "Wisconsin"
            meta.append({
                "generated_filename": generated_filename,
                "raw_url": raw_url,
                "pre_processed_url": build_raw_github_url(self.state, election['start_date'][:4], generated_filename),
                "ocd_id": ocd_id,
                "name": name,
                "election": election['slug']
            })
        return meta


    def _generate_filename(self, election):
        if election['special']:
            election_type = 'special__' + election['race_type']
        else:
            election_type = election['race_type']
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type,
            'ward'
        ]
        return "__".join(bits) + '.csv'

    def _jurisdictions(self):
        """Wisconsin counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']