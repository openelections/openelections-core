"""
Standardize names of data files on Wyoming Secretary of State.

The state offers XLS files containing precinct-level results for each county for all years except 2006.

These are represented in the dashboard API as the `direct_links` attribute on elections.

For 2006, precinct-level results are contained in county-specific PDF files. The CSV versions of those are contained in the 
https://github.com/openelections/openelections-data-wy repository.
"""
from os.path import join
import json
import datetime
import unicodecsv
import urlparse
import requests
from bs4 import BeautifulSoup

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

    def unprocessed_filename_url_pairs(self, year=None):
        return [(item['generated_filename'].replace(".csv", ".pdf"), item['raw_url'])
                for item in self.mappings(year)
                if item['pre_processed_url']]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        if year != 2006:
            for election in elections:
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    generated_filename = self._generate_office_filename(election['direct_links'][0], election['start_date'], election['race_type'], result)
                    meta.append({
                        "generated_filename": generated_filename,
                        "raw_url": self._build_raw_url(year, result['path']),
                        "pre_processed_url": build_github_url(self.state, generated_filename),
                        "ocd_id": 'ocd-division/country:us/state:wy',
                        "name": 'Wyoming',
                        "election": election['slug']
                    })
        else:
            for election in elections:
                csv_links = self._find_csv_links(election['direct_links'][0])
                counties = self._jurisdictions()
                results = zip(counties, csv_links[1:])
                for result in results:
                    meta.append({
                        "generated_filename": self._generate_county_filename(result[0]['county'], election),
                        "pre_processed_url": None,
                        "raw_url": result[1],
                        "ocd_id": result[0]['ocd_id'],
                        "name": result[0]['county'],
                        "election": election['slug']
                    })
        return meta
    
    def _build_raw_url(self, year, path):
        return "http://www.sos.wv.gov/elections/history/electionreturns/Documents/%s/%s" % (year, path)

    def _generate_statewide_filename(self, election):
        election_type = election['race_type']
        if election['special']:
            election_type = 'special__' + election_type
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        return "__".join(bits) + '.csv'
        
    def _generate_county_filename(self, county, election):
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election['race_type'],
            county.lower()
        ]
        return "__".join(bits) + '.csv'

    def _generate_office_filename(self, url, start_date, election_type, result):
        # example: 20120508__wv__primary__wirt.csv
        if result['district'] == '':
            office = result['office']
        else:
            office = result['office'] + '__' + result['district']
        if result['special']:
            election_type = 'special__' + election_type
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            office
        ]
        path = urlparse.urlparse(url).path
        name = "__".join(bits) + '.csv'
        return name

    def _generate_raw_file_paths(self, date):
        """Given an election date, return xls files from github repository"""
        string_date = date.strftime("%Y%m%d")
        


    def _jurisdictions(self):
        """Wyoming counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
