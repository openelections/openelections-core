"""
Rhode Island has a combination of text, Excel and HTML files containing city-level and precinct results for all offices
for all years back to 2000. Special election results are pre-processed and available on Github at
https://github.com/openelections/openelections-data-ri.
"""
from future import standard_library
standard_library.install_aliases()
from builtins import str
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
        """Return array of dicts containing source url and
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
            if 'special' in election['slug']:
                generated_filename = self._generate_filename(election, 'csv')
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": election['direct_links'][0],
                    "pre_processed_url": build_raw_github_url(self.state, str(year), generated_filename),
                    "ocd_id": 'ocd-division/country:us/state:ri',
                    "name": 'Rhode Island',
                    "election": election['slug']
                })
            else:
                result = [x for x in self._url_paths() if x['date'] == election['start_date']][0]
                if result['raw_extracted_filename']:
                    format = result['raw_extracted_filename'].split('.')[1]
                else:
                    format = 'csv'
                meta.append({
                    "generated_filename": self._generate_filename(election, format),
                    "raw_url": election['direct_links'][0],
                    "raw_extracted_filename": result['raw_extracted_filename'],
                    "pre_processed_url": None,
                    "ocd_id": 'ocd-division/country:us/state:ri',
                    "name": 'Rhode Island',
                    "election": election['slug']
                })

        return meta

    def _generate_filename(self, election, format):
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]
        if election['special'] == True:
            bits.extend(['special'])
        bits.extend(
            election['race_type'].split('-')
        )
        filename = "__".join(bits) + '.' + format
        return filename

    def _jurisdictions(self):
        """Rhode Island counties and municipalities"""
        return self.jurisdiction_mappings()

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
