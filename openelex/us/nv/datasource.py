"""
Nevada has CSV files containing precinct-level results for each jurisdiction and all offices
from the 2004 general to the 2012 general. All of those files are pre-processed and available on Github at
https://github.com/openelections/openelections-data-nv. Prior to the 2004 general and
after the 2012 primary, county-level XML and HTML files are available from the Secretary of State's site.
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
            if 'special' in election['slug']:
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == True]
                for result in results:
                    generated_filename = result['path']
                    if result['jurisdiction']:
                        jurisdiction = [c for c in self._jurisdictions() if c['jurisdiction'] == result['jurisdiction']][0]
                        ocd_id = jurisdiction['ocd_id']
                        name = jurisdiction['jurisdiction']
                    else:
                        ocd_id = 'ocd-division/country:us/state:nv'
                        name = 'Nevada'
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": result['url'],
                        "pre_processed_url": build_raw_github_url(self.state, str(year), result['path']),
                        "ocd_id": ocd_id,
                        "name": name,
                        "election": election['slug']
                    })
            else:
                # primary, general and runoff statewide elections have 1 or 2 files per county
                # some general runoffs will have smaller numbers of files
                results = [x for x in self._url_paths() if x['date'] == election['start_date'] and x['special'] == False]
                for result in results:
                    county = [c for c in self._jurisdictions() if c['jurisdiction'] == result['jurisdiction']][0]
                    generated_filename = self._generate_county_filename(election['start_date'], result)
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": result['url'],
                        "pre_processed_url": build_raw_github_url(self.state, str(year), result['path']),
                        "ocd_id": county['ocd_id'],
                        "name": result['county'],
                        "election": election['slug']
                    })
        return meta

    def _generate_county_filename(self, start_date, result):
        bits = [
            start_date.replace('-',''),
            self.state,
        ]
        if result['party']:
            bits.append(result['party'].lower())
        bits.extend([
            result['race_type'].lower(),
            result['jurisdiction'].replace(' ','_').lower()
        ])
        bits.append('precinct')
        filename = "__".join(bits) + '.csv'
        return filename

    def _jurisdictions(self):
        """Nevada counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['jurisdiction'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
