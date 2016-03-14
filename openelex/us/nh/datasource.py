"""
New Hampshire has Excel files containing precinct-level results for each county and office
for all years back to 2000, although some contests have a single statewide file. Belknap county
file can also have statewide summary results in it. NH has results by town, but larger towns
have multiple wards, so both towns and wards are considered "precinct-like" reporting levels.
"""
from os.path import join
import json
import datetime
import urlparse
import xlrd

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
            for result in results:
                if result['county'] == '':
                    generated_filename = self._generate_filename(election, result)
                else:
                    generated_filename = self._generate_county_filename(election, result)
                generated_ocd_id = self._generate_ocd_id(result)
                generated_name = self._generate_name(result)
                meta.append({
                    "generated_filename": generated_filename,
                    "raw_url": result['url'],
                    "ocd_id": generated_ocd_id,
                    "name": generated_name,
                    "election": election['slug']
                })
        return meta

    def _generate_filename(self, election, result):
        if result['district'] == '':
            office_district = result['office']
        else:
            office_district = result['office']+'__'+result['district'].replace('--','_')
        if result['party'] == '':
            bits = [
                    election['start_date'].replace('-',''),
                    self.state.lower(),
                    election['race_type'],
                    office_district
                ]
        else:
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                result['party'],
                election['race_type'],
                office_district
            ]
        name = "__".join(bits)+'.xls'
        return name

    def _generate_county_filename(self, election, result):
        if result['district'] == '':
            office_district = result['office']
        else:
            office_district = result['office']+'__'+result['district'].replace('--','_')
        if result['party'] == '':
            bits = [
                    election['start_date'].replace('-',''),
                    self.state.lower(),
                    election['race_type'],
                    result['county'].lower(),
                    office_district
                ]
        else:
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                result['party'],
                election['race_type'],
                result['county'].lower(),
                office_district
            ]
        name = "__".join(bits)+'.xls'
        return name

    def _generate_ocd_id(self, result):
        if result['ocd_id'] == '':
            return 'ocd-division/country:us/state:nh/county:'+result['county'].lower()
        else:
            return result['ocd_id']

    def _generate_name(self, result):
        if result['county'] == '':
            return 'New Hampshire'
        else:
            return result['county']

    def _jurisdictions(self):
        """New Hampshire counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _places(self):
        """New Hampshire places with political districts"""
        return self.place_mappings()
