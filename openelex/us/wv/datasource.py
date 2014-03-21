"""
Standardize names of data files on West Virginia Secretary of State and 
save to mappings/filenames.json

The state offers CSV files containing precinct-level results for each county by election date from 2008 onwards:

    http://apps.sos.wv.gov/elections/results/readfile.aspx?path=NC80LVN0YXRlQ291bnR5VG90YWxzLmNzdg==

These are represented in the dashboard API as the `direct_link` attribute on elections.

Prior to 2008, county-level results are contained in office-specific PDF files. The CSV versions of those are contained in the 
https://github.com/openelections/openelections-data-wv repository.
"""
import os
from os.path import join
import re
import json
import unicodecsv
import urlparse
import requests
from bs4 import BeautifulSoup

from openelex import PROJECT_ROOT
from openelex.api import elections as elec_api
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

    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            # Store elections by year
            self._elections = {}
            for elec in elec_api.find(self.state):
                rtype = elec['race_type'].lower()
                elec['slug'] = "-".join((self.state, elec['start_date'], rtype))
                yr = int(elec['start_date'][:4])
                self._elections.setdefault(yr, []).append(elec)
        if year:
            year_int = int(year)
            return {year_int: self._elections[year_int]}
        return self._elections

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        if year < 2008:
            for election in elections:
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    meta.append({
                        "generated_filename": self._generate_office_filename(election['direct_link'], election['start_date'], election['race_type'], result),
                        "raw_url": self._build_raw_url(year, result['path']),
                        "ocd_id": 'ocd-division/country:us/state:wv',
                        "name": 'West Virginia',
                        "election": election['slug']
                    })
        else:
            for election in elections:
                csv_links = self._find_csv_links(election['direct_link'])
                counties = self._jurisdictions()
                results = zip(counties, csv_links[1:])
                for result in results:
                    meta.append({
                        "generated_filename": self._generate_county_filename(result[0]['county'], election),
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
        if election['special'] == True:
            election_type = election_type + '__special'
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
        if result['special'] == '1':
            election_type = election_type + '__special'
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            office
        ]
        path = urlparse.urlparse(url).path
        name = "__".join(bits) + '.csv'
        return name
    
    def _find_csv_links(self, url):
        "Returns a list of dicts of counties and CSV formatted results files for elections 2008-present. First item is statewide, remainder are county-level."
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        return ['http://apps.sos.wv.gov/elections/results/'+x['href'] for x in soup.find_all('a') if x.text == 'Download Comma Separated Values (CSV)']

    def _url_paths(self):
        "Returns a JSON array of url path mappings"
        filename = join(PROJECT_ROOT, self.mappings_dir, 'url_paths.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)
    
    def _jurisdictions(self):
        """West Virginia counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings