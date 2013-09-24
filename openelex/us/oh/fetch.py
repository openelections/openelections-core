from openelex.base.fetch import BaseFetcher

"""
Retrieves CSV result files for a given year from Ohio Secretary of State and caches them locally.
General and primary election files have multiple jurisdictions and races in a single master file (via Excel sheets).

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

