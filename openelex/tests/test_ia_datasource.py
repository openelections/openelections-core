import json
from os.path import abspath, dirname, join
from unittest import TestCase

from openelex.us.ia.datasource import Datasource

## Mock API data
tests_dir = abspath(dirname(__file__))
fixture_path = join(tests_dir, 'fixtures/election_api_response_ia.json')
with open(fixture_path, 'r') as f:
    api_data = json.loads(f.read())

#    @patch('openelex.us.ia.datasource.elec_api.find')
#    def test_mappings_default(self, mock_elec_find):
#        # By default, mappings returns all URLs
#        mock_elec_find.return_value = api_data['objects']

class TestDatasource(TestCase):
    def setUp(self):
        self.datasource = Datasource()

    def test_precinct_xls_metadata(self):
        # There should be an entry for each of the 99 counties when
        # an election has this kind of results
        election = {
          'end_date': '2012-11-06',
          'race_type': 'general',
          'slug': 'ia-2012-11-06-general',
          'special': False,
          'start_date': '2012-11-06',
        }
        entries = self.datasource._precinct_xls_metadata(election) 
        self.assertEqual(len(entries), 99)

        # Check format of raw_url
        assert entries[0]['raw_url'].startswith("http://sos.iowa.gov/elections/results/xls/2012/general/") 

    def test_precinct_xls_base_url(self):
        election = {
          'slug': 'ia-2006-11-07-general',
          'start_date': '2006-11-07',
          'race_type': 'general',
        }
        base_url = self.datasource._precinct_xls_base_url(election)
        self.assertEqual(base_url, "http://sos.iowa.gov/elections/results/xls")

        election = {
          'slug': u'ia-2008-11-04-general',
          'start_date': u'2008-11-04',
          'race_type': 'general',
        }
        base_url = self.datasource._precinct_xls_base_url(election)
        self.assertEqual(base_url,
            "http://sos.iowa.gov/elections/results/xls/2008")

        election = {
            'slug': u'ia-2010-06-08-primary',
            u'start_date': u'2010-06-08',
            'race_type': 'primary',
        }
        base_url = self.datasource._precinct_xls_base_url(election)
        self.assertEqual(base_url,
            "http://sos.iowa.gov/elections/results/xls/2010/primary")

    def test_extension_from_url(self):
        url = "http://sos.iowa.gov/elections/pdf/results/2000s/2004SS30.pdf"
        ext = self.datasource._extension_from_url(url)
        self.assertEqual(ext, ".pdf")
