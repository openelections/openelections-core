"""
Louisiana has pre-processed county-level CSV files available on Github at
https://github.com/openelections/openelections-data-la.
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
            return mapping['url']
        except KeyError:
            return # github url

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            # parish-level file
            jurisdiction = 'ocd-division/country:us/state:la'
            generated_filename = self._generate_parish_filename(election)
            meta.append({
                "generated_filename": generated_filename,
                "raw_url": election['portal_link'],
                "pre_processed_url": build_raw_github_url(self.state, str(year), generated_filename),
                "ocd_id": jurisdiction,
                "name": 'Louisiana',
                "election": election['slug']
            })
            # precinct-level file
            jurisdiction = 'ocd-division/country:us/state:la'
            generated_filename = self._generate_precinct_filename(election)
            meta.append({
                "generated_filename": generated_filename,
                "raw_url": election['portal_link'],
                "pre_processed_url": build_raw_github_url(self.state, str(year), generated_filename),
                "ocd_id": jurisdiction,
                "name": 'Louisiana',
                "election": election['slug']
            })
        return meta

    def _generate_parish_filename(self, election):
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]
        if election['special']:
            bits.append('special')
        bits.extend([
            election['race_type'].replace('-','_').lower()
        ])
        filename = "__".join(bits) + '.csv'
        return filename

    def _generate_precinct_filename(self, election):
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]
        if election['special']:
            bits.append('special')
        bits.extend([
            election['race_type'].replace('-','_').lower(),
            'precinct'
        ])
        filename = "__".join(bits) + '.csv'
        return filename

    def _jurisdictions(self):
        """Louisiana parishes"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['name'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
