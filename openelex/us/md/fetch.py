"""
Retrieves CSV result files from Maryland State Board of Elections and caches them locally.
Accepts a datefilter argument to limit files that are downloaded.

USAGE

    from openelex.us.md import fetch
    f = fetch.FetchResults()
    f.run(2012)

"""
from openelex.base.fetch import BaseFetcher

class FetchResults(BaseFetcher):

    def run(self, datefilter=''):
        filenames = {}
        elections = self.api_response(self.state, year)
        for generated_name, raw_url, ocd_id, name, election in self.target_urls:
            self.fetch(raw_url, generated_name)
        filenames = [{ 'generated_name': generated_name, 'ocd_id' : ocd_id, 'raw_url' : raw_url, 'name' : name, 'election': election} for generated_name, raw_url, ocd_id, name, election in urls]
        self.update_mappings(year, filenames)
