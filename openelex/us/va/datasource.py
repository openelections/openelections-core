"""
Virginia has CSV files on an FTP site containing precinct-level results for each county and independent city
and all offices for all years back to 2005. Prior to 2005, they have CSV files with precinct and municipality-
level results available from http://historical.elections.virginia.gov/.
"""
from os.path import join
import json
import datetime
import urlparse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource

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
        return [(item['generated_filename'], item['raw_url'])
                for item in self.mappings(year)]

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            results = [x for x in self._url_paths() if x['date'] == election['start_date']]
            if len(results) > 0:
                for result in results:
                    generated_filename = self._generate_filename(election)
                    meta.append({
                        "generated_filename": result['path'],
                        "raw_url": result['url'],
                        "ocd_id": 'ocd-division/country:us/state:va',
                        "name": 'Virginia',
                        "election": election['slug']
                    })
            else:
                generated_filename = self._generate_filename(election)
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": election['direct_links'][0],
                    "ocd_id": 'ocd-division/country:us/state:va',
                    "name": 'Virginia',
                    "election": election['slug']
                })
        return meta

    def _generate_filename(self, election):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__") + '__precinct'
        else:
            election_type = election['race_type'].replace("-","__") + '__precinct'
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        name = "__".join(bits) + '.csv'
        return name

    def _jurisdictions(self):
        """Virginia counties and cities"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings
