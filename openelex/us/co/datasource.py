"""
New Mexico has precinct-level Excel files for the 2014 elections 
 are available on Github at https://github.com/openelections/openelections-data-co.
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
            return mapping['url']

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['url'] == url]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        for election in elections:
            if election['slug'] == 'nc-2000-05-02-primary':
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    generated_filename = self._generate_office_filename(election, result)
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": result['url'],
                        "ocd_id": 'ocd-division/country:us/state:co',
                        "name": 'Colorado',
                        "election": election['slug']
                    })
            else:
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    if result['date'] in ('2004-08-10', '2004-11-02', '2006-08-08', '2006-11-07', '2008-08-12', '2008-11-04', '2010-11-02', '2014-06-24', '2014-11-04'):
                        format = '.txt'
                    else:
                        format = '.csv'
                    generated_filename = self._generate_filename(election, format)
                    meta.append({
                        "generated_filename": generated_filename,
                        'raw_url': result['url'],
                        'raw_extracted_filename': result['raw_extracted_filename'],
                        "ocd_id": 'ocd-division/country:us/state:co',
                        "name": 'Colorado',
                        "election": election['slug']
                    })
        return meta

    def _generate_filename(self, election, format):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__")
        else:
            election_type = election['race_type'].replace("-","__")
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        name = "__".join(bits) + format
        return name

    def _jurisdictions(self):
        """Colorado counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['url']
