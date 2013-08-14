from openelex.base.fetch import BaseFetcher

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

class FetchResults(BaseFetcher):
    
    def run(self, year):
        # retrieve elections from api
        elections = self.api_response(self.state, year)
        urls = self.state_legislative_district_urls(year, elections)
        urls.update(self.county_urls(year, elections))
        for filename in urls.keys():
            # pass generated name into fetcher
            self.fetch(urls[filename], filename)
        # 
        # update_mappings(filenames)
        
    def state_legislative_district_urls(self, year, elections):
        urls = {}
        general = [e for e in elections if e['election_type'] == 'general'][0]
        generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__state_legislative.csv"
        raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_General.csv" % (year, year)
        urls[generated_name] = raw_name
        primary = [e for e in elections if e['election_type'] == 'primary'][0]
        for party in ['Democratic', 'Republican']:
            generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__general__state_legislative.csv"
            raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year)
            urls[generated_name] = raw_name
        return urls
    
    # add generated_name code here
    def county_urls(self, year, elections):
        urls = {}
        general = [e for e in elections if e['election_type'] == 'general'][0]
        primary = [e for e in elections if e['election_type'] == 'primary'][0]
        for jurisdiction in self.jurisdictions():
            county_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s.csv" % jurisdiction['url_name'].lower()
            county_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls[county_generated_name] = county_raw_name
            precinct_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s__precinct.csv" % jurisdiction['url_name'].lower()
            precinct_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls[precinct_generated_name] = precinct_raw_name
#            for party in ['Democratic', 'Republican']:
#                urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_%s_Primary.csv" % (year, jurisdiction, party, year))
#                urls.append("http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_%s_Primary.csv" % (year, jurisdiction, party, year))
        return urls
    
    def jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings(('ocd_id','fips','url_name'))
        mappings = [x for x in m if x['url_name'] is not None]
        return mappings
    
    # move this to base fetch.py
    def update_mappings(self, filenames):
        pass
        # store older mappings.json as a backup?
        # sort keys? use OrderedDict
        # from collections import OrderedDict
        #with open('mappings.json', 'w') as f:
            #f.write(standard_name, raw_name)
