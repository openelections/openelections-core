import os.path

from unittest import TestCase

from openelex.us.ar.fetch import FetchResults

class TestFetchResults(TestCase):
    def setUp(self):
        tests_dir = os.path.abspath(os.path.dirname(__file__))
        fixture_path = os.path.join(tests_dir, 'fixtures', 'ar_results_report_portal.html')
        with open(fixture_path, 'r') as f:
            self._portal_html = f.read()

        self._fetcher = FetchResults() 

    def test_scrape_contests(self):
        options = self._fetcher._scrape_contests(self._portal_html) 
        contest_names = [name for (contest_id, name) in options]
        self.assertEqual(len(options), 180)
        self.assertIn("U.S. President - Democrat", contest_names)
        self.assertIn("U.S. President - Republican", contest_names)
        self.assertIn("State Representative District 095 - Democrat",
            contest_names)
        self.assertIn("State Representative District 100 - Republican",
            contest_names)
