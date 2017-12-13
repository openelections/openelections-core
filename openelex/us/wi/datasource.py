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
from openelex.lib import build_raw_github_url

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
        return [(mapping['generated_filename'], self._url_for_fetch(mapping))
                for mapping in self.mappings(year)]

    def _url_for_fetch(self, mapping):
        try:
            return mapping['pre_processed_url']
        except KeyError:
            return mapping['raw_url']

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == election['special']]
            for result in results:
                if result['url']:
                    raw_url = result['url']
                else:
                    raw_url = None
                generated_filename = self._generate_filename(election['start_date'], election['race_type'], result)
                ocd_id = 'ocd-division/country:us/state:wi'
                name = "Wisconsin"
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": raw_url,
                    "pre_processed_url": build_raw_github_url(self.state, election['start_date'][0:4], generated_filename),
                    "ocd_id": ocd_id,
                    "name": name,
                    "election": election['slug']
                })
        return meta


    def _generate_filename(self, start_date, election_type, result):
        if result['special']:
            election_type = 'special__' + election_type
        bits = [
            start_date.replace('-',''),
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

