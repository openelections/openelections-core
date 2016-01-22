"""
Oregon has CSV files containing precinct-level results for each county and all offices
for all years back to 2000. All of the files are pre-processed and available on Github at
https://github.com/openelections/openelections-data-or.

For regular primary and general elections, there are statewide county-level files. Each county has a
precinct-level results file. Special and runoff elections for non-statewide offices are contained in a single file for each office.
"""
from os.path import join
import json
import datetime
import urlparse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url, build_raw_github_url

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
                    generated_filename = self._generate_filename(election['start_date'], election['race_type'], result)
                    ocd_id = 'ocd-division/country:us/state:or'
                    name = "Oregon"
                else:
                    generated_filename = self._generate_county_filename(election['start_date'], result)
                    ocd_id = 'ocd-division/country:us/state:or/county:%s' % result['county'].lower().replace(" ", "_")
                    name = result['county']
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": raw_url,
                    "pre_processed_url": build_raw_github_url(self.state, election['start_date'][0:4], generated_filename),
                    "ocd_id": ocd_id,
                    "name": name,
                    "election": election['slug']
                })
                # generate precinct files

        return meta

    def _generate_filename(self, start_date, election_type, result):
        if result['district'] == '':
            office = result['office'].lower().replace(' ','_')
        else:
            office = result['office'].lower().replace(' ','_') + '__' + result['district']
        if result['special']:
            election_type = 'special__' + election_type
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

    def _generate_county_filename(self, start_date, result):
        bits = [
            start_date.replace('-',''),
            self.state,
        ]
        if result['party']:
            bits.append(result['party'].lower())
        bits.extend([
            result['race_type'].lower(),
            result['county'].replace(' ','_').lower()
        ])
        bits.append('precinct')
        filename = "__".join(bits) + '.csv'
        return filename

    def _jurisdictions(self):
        """Oregon counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
