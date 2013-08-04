from openelex.base.fetch import BaseScraper
from openelex.us.md.geo import counties

# output is a file saved locally inside dir (git-ignored cache dir)
# make base_url and path and recreate directory structure

"""
File name for general election precinct-level files is county_name_by_precinct_year_general.csv
File name for general election state legislative district-level files is State_Legislative_Districts_year_general.csv
File name for general election county-level files is County_Name_party_year_general.csv

File name for primary election precinct-level files is County_Name_by_Precinct_party_year_Primary.csv
File name for primary election state legislative district-level files is State_Legislative_Districts_party_year_primary.csv
File name for primary election county-level files is County_Name_party_year_primary.csv

"""

class FetchResults(BaseScraper):

    def run(self, year):
        for url in self.state_legislative_district_urls(year=year):
            self.fetch(url)
    
    def state_legislative_district_urls(self, year):
        urls = []
        urls.append("http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_General.csv" % (year, year))
        for party in ['Democratic', 'Republican', 'Nonpartisan']:
            urls.append("http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year))
        return urls
    
        