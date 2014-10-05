from unittest import TestCase

from openelex.base.datasource import BaseDatasource

class MockIowaDatasource(BaseDatasource):
    """
    Mock datasource to test some base class bits

    The heavy lifting of a datasource is implemented in the subclasses.
    In order to test some of the methods on BaseDatasource, we need to
    implement some minimally functional methods in a datasource subclass.
    """
    def __init__(self, state='ia'):
        super(BaseDatasource, self).__init__(state)

    def mappings(self, year=None):
        mappings = [
            {
                'election': u'ia-2003-08-26-special-general',
                'generated_filename': u'20030826__ia__special__general__state_house__30__county.csv',
                'name': 'Iowa',
                'ocd_id': 'ocd-division/country:us/state:ia',
                'pre_processed_url': 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20030826__ia__special__general__state_house__30__county.csv',
                'raw_url': u'http://sos.iowa.gov/elections/pdf/results/2000s/2003HD30.pdf'
            },
            {
                'election': u'ia-2003-08-05-special-general',
                'generated_filename': u'20030805__ia__special__general__state_house__100__county.csv',
                'name': 'Iowa',
                'ocd_id': 'ocd-division/country:us/state:ia',
                'pre_processed_url': 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20030805__ia__special__general__state_house__100__county.csv',
                'raw_url': u'http://sos.iowa.gov/elections/pdf/results/2000s/2003HD100.pdf'
            },
            {
                'election': u'ia-2003-02-11-special-general',
                'generated_filename': u'20030211__ia__special__general__state_house__62__county.csv',
                'name': 'Iowa',
                'ocd_id': 'ocd-division/country:us/state:ia',
                'pre_processed_url': 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20030211__ia__special__general__state_house__62__county.csv',
                'raw_url': u'http://sos.iowa.gov/elections/pdf/results/2000s/2003HD62.pdf'
            },
            {
                'election': u'ia-2003-01-14-special-general',
                'generated_filename': u'20030114__ia__special__general__state_senate__26__county.csv',
                'name': 'Iowa',
                'ocd_id': 'ocd-division/country:us/state:ia',
                'pre_processed_url': 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20030114__ia__special__general__state_senate__26__county.csv',
                'raw_url': u'http://sos.iowa.gov/elections/pdf/results/2000s/2003SS26.pdf'
            }
        ]
        if year != "2003":
            return [] 

        return mappings

class TestBaseDatasource(TestCase):
    """Tests for the base datasource"""

    def setUp(self):
        self.datasource = MockIowaDatasource()

    def test_filename_year(self):
        filename = '20030114__ia__special__general__state_senate__26__county.csv' 
        self.assertEqual(self.datasource._filename_year(filename), '2003')

    def test_mapping_for_file(self):
        filename = '20030114__ia__special__general__state_senate__26__county.csv' 
        mapping = self.datasource.mapping_for_file(filename)
        self.assertEqual(mapping['election'], 'ia-2003-01-14-special-general')
        self.assertEqual(mapping['generated_filename'], filename)
        self.assertEqual(mapping['name'], "Iowa")
        self.assertEqual(mapping['ocd_id'], 'ocd-division/country:us/state:ia')
        self.assertEqual(mapping['pre_processed_url'], 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20030114__ia__special__general__state_senate__26__county.csv')
        self.assertEqual(mapping['raw_url'], 'http://sos.iowa.gov/elections/pdf/results/2000s/2003SS26.pdf')

        filename = 'bad_filename.csv'
        self.assertRaises(LookupError, self.datasource.mapping_for_file,
            filename)
