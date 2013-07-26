from openelex.base.fetch import BaseScraper

#from bs4 import BeautifulSoup


class FetchLA(BaseScraper):

    def run(self):
        links = self.target_links()
        for link in links:
            self.fetch(link)

    #TODO: scrape site and return list of links to target result pages or files
    def target_links(self):
        """Generate list of direct links to result pages or files"""
        links = []
        url = "http://staticresults.sos.la.gov/default.html"
        name, response = self.fetch(url)
        return links
