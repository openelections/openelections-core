from openelex.base.scrape import BaseScraper

# output is a file saved locally inside dir (git-ignored cache dir)
# make base_url and path and recreate directory structure

class FetchResults(BaseScraper):

    def __init__(self, *args, **kwargs):
        self.format = format # format of data ? 
        self.url = url # direct data link from dashboard
    
    def fetch(self, url):
        data = requests.get(url).content
        return data
    
    def run(self, url):
        self.fetch(url)
        