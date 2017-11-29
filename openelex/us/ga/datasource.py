"""
Stake in the ground for GA results
"""
from future import standard_library
standard_library.install_aliases()
from os.path import join
import json
import datetime
import urllib.parse

from openelex import PROJECT_ROOT
from openelex.lib import build_github_url
from openelex.base.datasource import BaseDatasource

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
        return [(item['generated_filename'], item['raw_url'])
                for item in self.mappings(year)]

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            if election['special']:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == True]
            else:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == False]
            for result in results:
                if result['url']:
                    raw_url = result['url']
                else:
                    raw_url = None
                if result['special']:
                    ocd_id = 'ocd-division/country:us/state:ga'
                    name = "Georgia"
                    generated_filename = self._generate_filename(election)
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": raw_url,
                        "pre_processed_url": build_github_url(self.state, generated_filename),
                        "ocd_id": ocd_id,
                        "name": 'Georgia',
                        "election": election['slug']
                    })
                    generated_filename = self._generate_special_filename(election, result)
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": raw_url,
                        "pre_processed_url": build_github_url(self.state, generated_filename),
                        "ocd_id": ocd_id,
                        "name": 'Georgia',
                        "election": election['slug']
                    })
                else:
                    generated_filename = self._generate_filename(election)
                    ocd_id = 'ocd-division/country:us/state:ga'
                    name = "Georgia"
                    for jurisdiction in self._jurisdictions():
                        generated_filename = self._generate_county_filename(election, jurisdiction['county'], result)
                        ocd_id = 'ocd-division/country:us/state:ga/county:%s' % result['county'].lower().replace(" ", "_")
                        meta.append({
                            "generated_filename": generated_filename,
                            "raw_url": raw_url,
                            "pre_processed_url": build_github_url(self.state, generated_filename),
                            "ocd_id": ocd_id,
                            "name": jurisdiction['county'],
                            "election": election['slug']
                        })
        return meta

    def _generate_filename(self, election):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__")
        else:
            election_type = election['race_type'].replace("-","__")
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        name = "__".join(bits) + '.csv'
        return name

    def _generate_county_filename(self, election, county, result):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__")
        else:
            election_type = election['race_type'].replace("-","__")
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
        ]
        if result['party']:
            bits.append(result['party'].lower())
        bits.extend([
            election_type,
            county.replace(' ','_').lower()
        ])
        bits.append('precinct')
        filename = "__".join(bits) + '.csv'
        return filename

    def _generate_special_filename(self, election, result):
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
        ]
        if result['party']:
            bits.append(result['party'].lower())
        bits.extend([
            'special__' + election['race_type'].replace("-","__")
        ])
        bits.append('precinct')
        filename = "__".join(bits) + '.csv'
        return filename

    def _jurisdictions(self):
        """Georgia counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings
