"""
Standardize names of data files on Idaho Department of State and 
save to mappings/filenames.json

File-name conventions on ID site are fairly consistent: Excel files containing county-level and precinct-level results:

    http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_fed_prec.xls
    http://www.sos.idaho.gov/elect/RESULTS/2012/General/12%20Gen_leg_prec.xls
    
although earlier files have a slightly different format:

    http://www.sos.idaho.gov/elect/2000rslt/primary/00_pri_fed.xls
    http://www.sos.idaho.gov/elect/2000rslt/general/00_gen_lgpc.xls

There are two files for each election; one covers federal and statewide offices, with the other covers the state legislature. We put these into a dict
structured like so:
    
    {'primary_2012_statewide':'http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_fed_prec.xls', 'primary_2012_state_legislature':'http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_leg_prec.xls'}

"""
import os
from os.path import join
import re
import json
import unicodecsv
import requests
from bs4 import BeautifulSoup

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
    
    def results_links(self):
        url = "http://www.sos.idaho.gov/elect/results.htm"
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        table = soup.find_all('table')[3]
        precincts = [x for x in table.find_all('a') if 'prec' in x['href'] or 'pct' in x['href']]
        self._results_links = [x for x in precincts if x.text == 'Statewide' or x.text == 'Legislature']
        return self._results_links
        
    # PRIVATE METHODS
    
    def __filter_results_links(self, year):
        links = {}
        for link in [x for x in self._results_links if str(year) in x['href']]:
            if 'primary' in link['href'] and link.text == 'Statewide':
                slug = 'primary_%s_statewide' % str(year)
                links[slug] = 'http://www.sos.idaho.gov/elect/'+link['href']
            elif 'primary' in link['href']:
                slug = 'primary_%s_state_legislature' % str(year)
                links[slug] = 'http://www.sos.idaho.gov/elect/'+link['href']
            elif 'general' in link['href'] and link.text == 'Statewide':
                slug = 'general_%s_statewide' % str(year)
                links[slug] = 'http://www.sos.idaho.gov/elect/'+link['href']
            else:
                slug = 'general_%s_state_legislature' % str(year)
                links[slug] = 'http://www.sos.idaho.gov/elect/'+link['href']
        return links
    
    def __build_absentee_metadata(self, year, election):
        # absentee files available from 2002-forward
        if election['race_type'] == 'general':
            slug = str(year)[2:4] + 'Gen'
        else:
            slug = str(year)[2:4] + 'Pri'
        raw_url = "http://www.sos.idaho.gov/elect/absentee/%s_Absentee.xls" % slug
        return {
                    "generated_filename": self._generate_filename(election, absentee=True),
                    "raw_url": raw_url,
                    "ocd_id": 'ocd-division/country:us/state:id',
                    "name": 'Idaho',
                    "election": election['slug']
                }

    def _build_metadata(self, year, elections):
        # TODO: need to handle multiple files per election
        # ID has two files for each election
        meta = []
        year_int = int(year)
        results_links = self.__filter_results_links(year)
        for election in elections:
            if year > 2000:
                meta.append(self.__build_absentee_metadata(year, election))
            for link in results_links:
                meta.append({
                    "generated_filename": self._generate_filename(election),
                    "raw_url": election['direct_link'],
                    "ocd_id": 'ocd-division/country:us/state:id',
                    "name": 'Idaho',
                    "election": election['slug']
                })
        return meta
    
    def _generate_filename(self, election, absentee=False):
        race_type = election['race_type']
        if election['special'] == True:
            race_type = race_type + '__special'
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            race_type
        ]
        if absentee:
            bits.append('absentee')
        name = "__".join(bits) + '.xls'
        return name
    
    def _jurisdictions(self):
        """Idaho counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m]
        return mappings
