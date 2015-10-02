"""
The state offers CSV files containing precinct-level results for regularly scheduled general elections from 2002-2010; primary and
special elections are pre-processed CSV files from HTML/PDF/Excel files. All of those files are available in the
https://github.com/openelections/openelections-data-ct repository.
"""
from os.path import join
import json
import unicodecsv
import urlparse
import requests

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url

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
        return [(item['generated_filename'], self._url_for_fetch(item))
                for item in self.mappings(year)]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            if election['special']:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == True]
            else:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] != True]
            for result in results:
                generated_filename = self._generate_filename(election['start_date'], election['race_type'], result)
                if election['direct_links']:
                    raw_url = election['direct_links'][0]
                    github_url = None
                else:
                    raw_url = None
                    github_url = build_github_url(self.state, generated_filename)
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": raw_url,
                    "pre_processed_url": github_url,
                    "ocd_id": 'ocd-division/country:us/state:ct',
                    "name": 'Connecticut',
                    "election": election['slug']
                })
        return meta

    def _generate_filename(self, start_date, election_type, result):
        if result['district'] == '':
            office = result['office']
        else:
            office = result['office'] + '__' + result['district']
        if result['special']:
            election_type = 'special__' + election_type
        else:
            election_type = election_type+'__precinct'
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            office
        ]
        if office == '':
            bits.remove(office)
        name = "__".join(bits) + '.csv'
        return name

    def _jurisdictions(self):
        """Pennsylvania counties"""
        return self.jurisdiction_mappings()

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
