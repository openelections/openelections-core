from openelex.base.fetch import BaseScraper
from openelex.us.md.geo import jurisdictions
import urlparse

"""
Retrieves CSV result files for a given year from Maryland State Board of Elections and caches them locally.

File name for general election precinct-level files is county_name_by_precinct_year_general.csv
File name for general election state legislative district-level files is State_Legislative_Districts_year_general.csv
File name for general election county-level files is County_Name_party_year_general.csv

File name for primary election precinct-level files is County_Name_by_Precinct_party_year_Primary.csv
File name for primary election state legislative district-level files is State_Legislative_Districts_party_year_primary.csv
File name for primary election county-level files is County_Name_party_year_primary.csv

Usage:

from openelex.us.md import fetch
f = fetch.FetchResults()
f.run(2012)
"""

class FetchResults(BaseScraper):
    
    def run(self, year):
        # retrieve elections from api
        openelex_elections = self.api_response(self.state, year)
        urls = self.state_legislative_district_urls(year, openelex_elections)
        urls.update(self.county_urls(year, openelex_elections))
        for url in urls.keys():
            result = urlparse.urlsplit(url)
            self.fetch(url, result.path.split('/')[4])
            update_mappings(url, urls[url])
        
    def state_legislative_district_urls(self, year, elections):
        urls = {}
        general = [e for e in elections if e['election_type'] == 'general'][0]
        raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_General.csv" % (year, year)
        urls[raw_name] = general['start_date'].replace('-','')+"__"+state+"__general__state_legislative.csv"
        primary = [e for e in elections if e['election_type'] == 'primary'][0]
        for party in ['Democratic', 'Republican']:
            raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year)
            urls[raw_name] = primary['start_date'].replace('-','')+"__"+state+"__general__state_legislative.csv"
        return urls
    
    def county_urls(self, year):
        urls = []
        for jurisdiction in jurisdictions():
            urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_General.csv" % (year, jurisdiction, year))
            urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_General.csv" % (year, jurisdiction, year))
            for party in ['Democratic', 'Republican']:
                urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_%s_Primary.csv" % (year, jurisdiction, party, year))
                urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_%s_Primary.csv" % (year, jurisdiction, party, year))
        return urls
    
    def update_mappings(self, raw_name, standard_name):
        # should this check to see if name pair is already in file?
        with open('mappings.txt', 'a') as f:
            f.write(standard_name, raw_name)
