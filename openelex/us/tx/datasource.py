"""
Texas has CSV files containing county-level and statewide results for all offices
for all years back to 2000. All of the files are pre-processed and available on Github at
https://github.com/openelections/openelections-data-tx.
"""
from os.path import join
import json
import datetime
import urlparse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib import build_raw_github_url

class Datasource(BaseDatasource):

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in self.elections(year).items():
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
            # each election has a statewide and a county pre-processed file.
            statewide_filename = self._generate_statewide_filename(election)
            meta.append({
                "generated_filename": statewide_filename,
                "raw_url": election['direct_links'][0],
                "pre_processed_url": build_raw_github_url(self.state, str(year), statewide_filename),
                "ocd_id": 'ocd-division/country:us/state:tx',
                "name": 'Texas',
                "election": election['slug']
            })

            county_filename = self._generate_county_filename(election)
            meta.append({
                "generated_filename": county_filename,
                "raw_url": election['direct_links'][0],
                "pre_processed_url": build_raw_github_url(self.state, str(year), county_filename),
                "ocd_id": 'ocd-division/country:us/state:tx',
                "name": 'Texas',
                "election": election['slug']
            })

        return meta

    def _generate_statewide_filename(self, election):
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]
        if election['special'] == True:
            bits.extend(['special'])
        bits.extend(
            election['race_type'].split('-')
        )
        filename = "__".join(bits) + '.csv'
        return filename

    def _generate_county_filename(self, election):
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]
        if election['special'] == True:
            bits.extend(['special'])
        bits.extend(
            election['race_type'].split('-')
        )
        bits.extend(['county'])
        filename = "__".join(bits) + '.csv'
        return filename

    def _jurisdictions(self):
        """Texas counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
