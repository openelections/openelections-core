from unittest import TestCase

from openelex.us.wa.datasource import Datasource

class TestDatasource(TestCase):
    def setUp(self):
        self.datasource = Datasource()

    def test_reporting_level_from_url(self):
        urls = [
            ("https://wei.sos.wa.gov/agency/osos/en/press_and_research/PreviousElections/2007/Primary/Documents/2007Prim%20Statewide%20Results_FINAL.xls", 'state'),
            ("https://wei.sos.wa.gov/agency/osos/en/press_and_research/PreviousElections/2007/Primary/Documents/2007Prim%20County%20Results.xls", 'county'),
        ]

        for url, expected in urls:
            reporting_level = self.datasource._reporting_level_from_url(url)
            self.assertEqual(reporting_level, expected)
