from openelex.base.fetch import BaseFetcher

"""
Retrieves CSV result files for a given year from Ohio Secretary of State and caches them locally.
General and primary election files have multiple jurisdictions and races in a single master file (via Excel sheets).

Most complete data is in precinct file, which has sheet for all counties and then sheets for each county. Likely we'll want the county
sheets because the all counties sheet also lists candidates who did not run in every county.

URL structure for precinct files: 
    General - http://www.sos.state.oh.us/sos/upload/elections/[year]/gen/[year]precinct.xlsx
    Primary - http://www.sos.state.oh.us/sos/upload/elections/[year]/pri/[year]precinct.xlsx

Although there are variations, so probably better to use Dashboard API to get download urls. Special elections need to be scraped.

Usage:

from openelex.us.oh import fetch
f = fetch.FetchResults()
f.run(2012)
"""

class FetchResults(BaseFetcher):
    
    def run(self, year, urls=None):
        filenames = {}
        elections = self.api_response(self.state, year)
        if not urls:
            urls = self.state_legislative_district_urls(year, elections) + self.county_urls(year, elections)
        for generated_name, raw_url, ocd_id, name in urls:
            self.fetch(raw_url, generated_name)
        filenames = [{ 'generated_name': generated_name, 'ocd_id' : ocd_id, 'raw_url' : raw_url, 'name' : name} for generated_name, raw_url, ocd_id, name in urls]
        self.update_mappings(year, filenames)
    
    def county_urls(year, elections):
        pass
        # build urls for county-level xlsx files
    
    def precinct_urls(year, elections):
        pass
        # build urls for precinct-level xlsx files
        
    def jurisdictions(self):
        """Ohio counties"""
        m = self.jurisdiction_mappings(('ocd_id','name'))
        mappings = [x for x in m if 'County' in x['name']]
        return mappings


