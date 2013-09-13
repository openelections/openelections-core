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
    
    def run(self, year, urls=None):
        elections = self.api_response(self.state, year)
        if not urls:
            urls = self.state_legislative_district_urls(year, elections) + self.county_urls(year, elections)
        for generated_name, raw_url, ocd_id in urls:
            self.fetch(raw_url, generated_name)
        filenames = [{ 'generated_name': generated_name, 'ocd_id' : ocd_id, 'raw_url' : raw_url} for generated_name, raw_url, ocd_id in urls]
        self.update_mappings(year, filenames)
        
    def state_legislative_district_urls(self, year, elections):
        urls = []
        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
        generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__state_legislative.csv"
        raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_General.csv" % (year, year)
        urls.append([generated_name, raw_name, 'ocd-division/country:us/state:md/sldl:all'])
        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]
        for party in ['Democratic', 'Republican']:
            generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__general__state_legislative.csv"
            raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year)
            urls.append([generated_name, raw_name, 'ocd-division/country:us/state:md/sldl:all'])
        return urls
    
    # add generated_name code here
    def county_urls(self, year, elections):
        urls = []
        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]
        for jurisdiction in self.jurisdictions():
            county_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s.csv" % jurisdiction['url_name'].lower()
            county_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([county_generated_name, county_raw_name, jurisdiction['ocd_id']])
            precinct_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s__precinct.csv" % jurisdiction['url_name'].lower()
            precinct_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([precinct_generated_name, precinct_raw_name, jurisdiction['ocd_id']])
            for party in ['Democratic', 'Republican']:
                county_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__primary__%s.csv" % jurisdiction['url_name'].lower()
                county_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_%s_Primary.csv" % (year, jurisdiction, party, year)
                urls.append([county_party_generated_name, county_party_raw_name, jurisdiction['ocd_id']])
                precinct_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__primary__%s__precinct.csv" % jurisdiction['url_name'].lower()
                precinct_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_%s_Primary.csv" % (year, jurisdiction, party, year)
                urls.append([precinct_party_generated_name, precinct_party_raw_name, jurisdiction['ocd_id']])
        return urls
    
    def jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings(('ocd_id','fips','url_name'))
        mappings = [x for x in m if x['url_name'] is not None]
        return mappings
        
