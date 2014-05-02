import os.path
from unittest import TestCase

from openelex.us.ar.datasource import Datasource

tests_dir = os.path.abspath(os.path.dirname(__file__))
fixture_dir = os.path.join(tests_dir, 'fixtures') 

class TestDatasource(TestCase):
    def setUp(self):
        self.datasource = Datasource()

    def test_clarity_election_base_url(self):
        # raw, base
        urls = [
            ("http://results.enr.clarityelections.com/AR/39376/83979/en/reports.html",
             "http://results.enr.clarityelections.com/AR/39376/83979/"),
            ("http://results.enr.clarityelections.com/AR/Arkansas/42845/index.html",
             "http://results.enr.clarityelections.com/AR/Arkansas/42845/"),
        ]

        for url, expected in urls:
            base_url = self.datasource._clarity_election_base_url(url)
            self.assertEqual(base_url, expected)

    def test_scrape_county_paths(self):
        fixture_path = os.path.join(fixture_dir, 'ar_results_clarity_select_county.html')
        with open(fixture_path, 'r') as f:
            html = f.read()
            paths = self.datasource._scrape_county_paths(html)
            self.assertEqual(len(paths), 75)

    def test_scrape_county_redirect(self):
        fixture_path = os.path.join(fixture_dir, 
            'ar_results_clarity_county_redirect.html')
        with open(fixture_path, 'r') as f:
            html = f.read()
            path = self.datasource._scrape_county_redirect_path(html)
            self.assertEqual(path, '112821/summary.html')

    def test_clariy_county_url(self):
        pass
