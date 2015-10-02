"""
Stake in the ground for GA results
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
            print election
            if election['special']:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == True]
            else:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == False]
            for result in results:
                if result['url']:
                    raw_url = result['url']
                else:
                    raw_url = None
                if result['county'] == '':
                    generated_filename = self._generate_filename(election['start_date'], result)
                    ocd_id = 'ocd-division/country:us/state:ga'
                    name = "Georgia"
                else:
                    generated_filename = self._generate_county_filename(election['start_date'], result)
                    ocd_id = 'ocd-division/country:us/state:ga/county:%s' % result['county'].lower().replace(" ", "_")
                    name = result['county']
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": raw_url,
                    "pre_processed_url": build_github_url(self.state, generated_filename),
                    "ocd_id": ocd_id,
                    "name": name,
                    "election": election['slug']
                })
        return meta

    def _generate_filename(self, election, format):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__") + '__precinct'
        else:
            election_type = election['race_type'].replace("-","__") + '__precinct'
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        name = "__".join(bits) + format
        return name

    def _generate_office_filename(self, election, result):
        if result['party'] == '':
            bits = [
                    election['start_date'].replace('-',''),
                    self.state.lower(),
                    election['race_type'],
                    result['office'],
                    'precinct'
                ]
        else:
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                result['party'],
                election['race_type'],
                result['office'],
                'precinct'
            ]
        name = "__".join(bits)+'.xls'
        return name

    def _jurisdictions(self):
        """Georgia counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings
