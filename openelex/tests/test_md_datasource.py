from os.path import abspath, dirname, join
from unittest import TestCase
import json

from mock import patch

from openelex.us.md.datasource import Datasource

## Mock api data
tests_dir = abspath(dirname(__file__))
fixture_path = join(tests_dir, 'fixtures/election_api_response_md.json')
with open(fixture_path, 'r') as f:
    md_data = json.loads(f.read())


class TestMappings(TestCase):

    def setUp(self):
        self.datasource = Datasource()

    @patch('openelex.base.datasource.elec_api.find')
    def test_mappings_default(self, mock_elec_find):
        # By default, mappings returns all URLs
        mock_elec_find.return_value = md_data['objects']
        mappings = self.datasource.mappings()
        expected_2000 = {
            'election': u'md-2000-11-07-general',
            'generated_filename': u'20001107__md__general__state_legislative.csv',
            'name': 'State Legislative Districts',
            'ocd_id': 'ocd-division/country:us/state:md/sldl:all',
            'raw_url': 'http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv'
        }
        self.assertDictEqual(expected_2000, mappings[0])
        expected_2012 = {
            'election': u'md-2012-04-03-primary',
            'generated_filename': u'20120403__md__republican__primary__baltimore_city.csv',
            'name': u'Baltimore City',
            'ocd_id': u'ocd-division/country:us/state:md/place:baltimore',
            'raw_url': u'http://www.elections.state.md.us/elections/2012/election_data/Baltimore_City_County_Republican_2012_Primary.csv'
        }
        self.assertDictEqual(expected_2012, mappings[-1])

    @patch('openelex.base.datasource.elec_api.find')
    def test_mappings_filtered_by_year(self, mock_elec_find):
        mock_elec_find.return_value = md_data['objects']
        mappings = self.datasource.mappings(2000)
        expected_2000 = {
            'election': u'md-2000-11-07-general',
            'generated_filename': u'20001107__md__general__state_legislative.csv',
            'name': 'State Legislative Districts',
            'ocd_id': 'ocd-division/country:us/state:md/sldl:all',
            'raw_url': 'http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv'
        }
        self.assertDictEqual(expected_2000, mappings[0])
        self.assertEqual(len(mappings), 50)

class TestTargetUrls(TestCase):

    def setUp(self):
        self.datasource = Datasource()

    @patch('openelex.base.datasource.elec_api.find')
    def test_target_urls_default(self, mock_elec_find):
        # By default, target_urls returns all URLs
        mock_elec_find.return_value = md_data['objects']
        target_urls = self.datasource.target_urls()
        expected_urls = [
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv",
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv",
            "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv",
            "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv",
            "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv",
            "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt",
            "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt",

        ]
        for url in expected_urls:
            self.assertIn(url, target_urls)

    @patch('openelex.base.datasource.elec_api.find')
    def test_target_urls_filtered_by_year(self, mock_elec_find):
        # supplying only a year returns a state legislative url for a general election
        mock_elec_find.return_value = md_data['objects']
        target_urls = self.datasource.target_urls(2012)
        unexpected_urls = [
            "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv",
            "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv",
            "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt",
            "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt",

        ]
        # 2012 URLs should be there
        self.assertIn("http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv", target_urls)
        self.assertIn("http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv", target_urls)
        # and other years should not
        for url in unexpected_urls:
            self.assertNotIn(url, target_urls)

class TestUrlFilenameMappings(TestCase):

    def setUp(self):
        self.datasource = Datasource()

    @patch('openelex.base.datasource.elec_api.find')
    def test_filename_url_pairs_default(self, mock_elec_find):
        # By default, ls returns all URLs
        expected = [
            ("20121106__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv"),
            ("20120403__md__democratic__primary__state_legislative.csv", "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv"),
            ("20121106__md__general__allegany.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv"),
            ("20121106__md__general__allegany__precinct.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv"),
            ("20120403__md__democratic__primary__allegany.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv"),
            ("20120403__md__democratic__primary__allegany__precinct.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv"),
            ("20001107__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv"),
            ("20041102__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv"),
            ("20041102__md__general__allegany.csv", "http://www.elections.state.md.us/elections/2004/election_data/Allegany_General_2004.csv"),
            ("20040302__md__democratic__primary__allegany.csv", "http://www.elections.state.md.us/elections/2004/election_data/Allegany_Democratic_Primary_2004.csv"),
            ("20021105__md__general.txt", "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"),
            ("20020910__md__primary.txt", "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"),
        ]
        mock_elec_find.return_value = md_data['objects']
        pairs = self.datasource.filename_url_pairs()
        urls = set([pair[1] for pair in pairs])
        for pair in expected:
            self.assertTrue(pair[1] in urls)

    @patch('openelex.base.datasource.elec_api.find')
    def test_filename_url_pairs_filterd_by_year(self, mock_elec_find):
        # supplying only a year returns a state legislative url for a general election
        mock_elec_find.return_value = md_data['objects']
        pairs = self.datasource.filename_url_pairs(2012)
        expected = [
            ("20121106__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv"),
            ("20120403__md__democratic__primary__state_legislative.csv", "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv"),
            ("20121106__md__general__allegany.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv"),
            ("20121106__md__general__allegany__precinct.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv"),
            ("20120403__md__democratic__primary__allegany.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv"),
            ("20120403__md__democratic__primary__allegany__precinct.csv", "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv"),
        ]
        unexpected = [
            ("20001107__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv"),
            ("20041102__md__general__state_legislative.csv", "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv"),
            ("20041102__md__general__allegany.csv", "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_General_2004.csv"),
            ("20040302__md__democratic__primary__allegany.csv", "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_Democratic_Primary_2004.csv"),
            ("20021105__md__general.txt", "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"),
            ("20020910__md__primary.txt", "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"),
        ]
        # 2012 pairs should be there
        for pair in expected:
            self.assertIn(pair, pairs)
        # and other years should not
        for pair in unexpected:
            self.assertNotIn(pair, pairs)

class TestSourceUrlBuilder(TestCase):

    def setUp(self):
        self.datasource = Datasource()

    def test_build_state_leg_urls(self):
        # supplying only a year returns a state legislative url for a general election
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv",
            self.datasource._build_state_leg_url(2012)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv",
            self.datasource._build_state_leg_url(2012, 'Democratic')
        )
        # 2000 and 2004 end with the year
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv",
            self.datasource._build_state_leg_url(2004)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv",
            self.datasource._build_state_leg_url(2000)
        )

    def test_build_county_urls(self):
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv",
            self.datasource._build_county_url(2012, 'Allegany')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv",
            self.datasource._build_county_url(2012, 'Allegany', precinct=True)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv",
            self.datasource._build_county_url(2012, 'Allegany', 'Democratic')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv",
            self.datasource._build_county_url(2012, 'Allegany', 'Democratic', precinct=True)
        )
        # 2000 and 2004 files end with the 4-digit year and do not use the _County"
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/Allegany_General_2004.csv",
            self.datasource._build_county_url(2004, 'Allegany')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/Allegany_Democratic_Primary_2004.csv",
            self.datasource._build_county_url(2004, 'Allegany', 'Democratic')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2000/election_data/Allegany_General_2000.csv",
            self.datasource._build_county_url(2000, 'Allegany')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2000/election_data/Allegany_By_Precinct_General_2000.csv",
            self.datasource._build_county_url(2000, 'Allegany', precinct=True)
        )

    def test_2002_source_urls(self):
        self.assertListEqual(
            ["http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt",
            "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"],
            self.datasource._get_2002_source_urls()
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt",
            self.datasource._get_2002_source_urls('general')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt",
            self.datasource._get_2002_source_urls('primary')
        )

class TestStandardizedFilenames(TestCase):
    """
    Standardized filenames are the names under which source data
    will be stored both locally during development and on S3
    for archival purposes.
    """

    def setUp(self):
        self.datasource = Datasource()
        self.allegany_jurisdiction = {
            'ocd_id': 'ocd-division/country:us/state:md/county:allegany',
            'fips': '24001',
            'name':'Allegany',
            'url_name': 'allegany'
        }

    def test_state_leg_filename_general_racewide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv"
        actual = self.datasource._generate_state_leg_filename(raw_url, '2012-11-06')
        self.assertEquals('20121106__md__general__state_legislative.csv', actual)

    def test_state_leg_filename_primary_racewide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv"
        actual = self.datasource._generate_state_leg_filename(raw_url, '2012-04-03')
        self.assertEquals("20120403__md__democratic__primary__state_legislative.csv", actual)

    def test_county_filename_general_county_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2012-11-06', self.allegany_jurisdiction)
        self.assertEquals("20121106__md__general__allegany.csv", actual)

    def test_county_filename_general_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2012-11-06', self.allegany_jurisdiction)
        self.assertEquals("20121106__md__general__allegany__precinct.csv", actual)

    def test_county_filename_primary_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2012-04-03', self.allegany_jurisdiction)
        self.assertEquals("20120403__md__democratic__primary__allegany.csv", actual)

    def test_county_filename_primary_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2012-04-03', self.allegany_jurisdiction)
        self.assertEquals("20120403__md__democratic__primary__allegany__precinct.csv", actual)

    # 2000 and 2004 files end with the 4-digit year
    def test_2000_state_leg_filename_general_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv"
        actual = self.datasource._generate_state_leg_filename(raw_url, '2000-11-07')
        self.assertEquals("20001107__md__general__state_legislative.csv",  actual)

    def test_2004_state_leg_filename_general_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv"
        actual = self.datasource._generate_state_leg_filename(raw_url, '2004-11-02')
        self.assertEquals("20041102__md__general__state_legislative.csv", actual)

    def test_2004_county_filename_general_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_General_2004.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2004-11-02', self.allegany_jurisdiction)
        self.assertEquals("20041102__md__general__allegany.csv", actual)

    def test_2004_county_filename_primary_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_Democratic_Primary_2004.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2004-03-02', self.allegany_jurisdiction)
        self.assertEquals("20040302__md__democratic__primary__allegany.csv", actual)

    def test_2000_county_filename_general_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2000/election_data/Allegany_County_General_2000.csv"
        actual = self.datasource._generate_county_filename(raw_url, '2000-11-07', self.allegany_jurisdiction)
        self.assertEquals("20001107__md__general__allegany.csv", actual)

    def test_2000_county_filename_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2000/election_data/Allegany_By_Precinct_2000.csv"
        # there are no 2000 precinct results so this should raise an error
        self.assertRaises(
            AttributeError,
            self.datasource._generate_county_filename,
            raw_url, '2000-11-07', self.allegany_jurisdiction,
        )

    # 2002 only has two files -- primary and general
    def test_2002_general_filename(self):
        raw_url = "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"
        actual = self.datasource._generate_2002_filename(raw_url)
        self.assertEquals("20021105__md__general.txt", actual)

    def test_2002_primary_filename(self):
        raw_url = "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"
        actual = self.datasource._generate_2002_filename(raw_url)
        self.assertEquals("20020910__md__primary.txt", actual)
