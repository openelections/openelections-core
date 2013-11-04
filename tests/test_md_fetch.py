from unittest import TestCase

from openelex.us.md.fetch import FetchResults

class TestSourceUrlBuilder(TestCase):

    def setUp(self):
        self.fetcher = FetchResults()

    def test_build_state_leg_urls(self):
        # supplying only a year returns a state legislative url for a general election
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv",
            self.fetcher.build_state_leg_url(2012)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv",
            self.fetcher.build_state_leg_url(2012, 'Democratic')
        )
        # 2000 and 2004 end with the year
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv",
            self.fetcher.build_state_leg_url(2004)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv",
            self.fetcher.build_state_leg_url(2000)
        )

    def test_build_county_urls(self):
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv",
            self.fetcher.build_county_url(2012, 'Allegany')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv",
            self.fetcher.build_county_url(2012, 'Allegany', precinct=True)
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv",
            self.fetcher.build_county_url(2012, 'Allegany', 'Democratic')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv",
            self.fetcher.build_county_url(2012, 'Allegany', 'Democratic', precinct=True)
        )
        # 2000 and 2004 files end with the 4-digit year
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_General_2004.csv",
            self.fetcher.build_county_url(2004, 'Allegany')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_Democratic_Primary_2004.csv",
            self.fetcher.build_county_url(2004, 'Allegany', 'Democratic')
        )

    def test_2002_source_urls(self):
        "2002 source url returns url for general or primary all-inclusive file"
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt",
            self.fetcher.get_2002_source_url('general')
        )
        self.assertEquals(
            "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt",
            self.fetcher.get_2002_source_url('primary')
        )

class TestStandardizedFilenames(TestCase):
    """
    Standardized filenames are the names under which source data
    will be stored both locally during development and on S3
    for archival purposes.
    """

    def setUp(self):
        self.fetcher = FetchResults()
        self.allegany_jurisdiction = {
            'ocd_id': 'ocd-division/country:us/state:md/county:allegany',
            'fips': '24001',
            'name':'Allegany',
            'url_name': 'allegany'
        }

    def test_state_leg_filename_general_racewide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_2012_General.csv"
        actual = self.fetcher.generate_state_leg_filename(raw_url, '2012-11-06')
        self.assertEquals('20121106__md__general__state_legislative.csv', actual)

    def test_state_leg_filename_primary_racewide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/State_Legislative_Districts_Democratic_2012_Primary.csv"
        actual = self.fetcher.generate_state_leg_filename(raw_url, '2012-04-03')
        self.assertEquals("20120403__md__democratic__primary__state_legislative.csv", actual)

    def test_county_filename_general_county_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_2012_General.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2012-11-06', self.allegany_jurisdiction)
        self.assertEquals("20121106__md__general__allegany.csv", actual)

    def test_county_filename_general_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_2012_General.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2012-11-06', self.allegany_jurisdiction)
        self.assertEquals("20121106__md__general__allegany__precinct.csv", actual)

    def test_county_filename_primary_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_County_Democratic_2012_Primary.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2012-04-03', self.allegany_jurisdiction)
        self.assertEquals("20120403__md__democratic__primary__allegany.csv", actual)

    def test_county_filename_primary_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2012/election_data/Allegany_By_Precinct_Democratic_2012_Primary.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2012-04-03', self.allegany_jurisdiction)
        self.assertEquals("20120403__md__democratic__primary__allegany__precinct.csv", actual)

    # 2000 and 2004 files end with the 4-digit year
    def test_2000_state_leg_filename_general_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2000/election_data/State_Legislative_Districts_General_2000.csv"
        actual = self.fetcher.generate_state_leg_filename(raw_url, '2000-11-07')
        self.assertEquals("20001107__md__general__state_legislative.csv",  actual)

    def test_2004_state_leg_filename_general_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/State_Legislative_Districts_General_2004.csv"
        actual = self.fetcher.generate_state_leg_filename(raw_url, '2004-11-02')
        self.assertEquals("20041102__md__general__state_legislative.csv", actual)

    def test_2004_county_filename_general_countywide_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_General_2004.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2004-11-02', self.allegany_jurisdiction)
        self.assertEquals("20041102__md__general__allegany.csv", actual)

    def test_2004_county_filename_primary_precinct_results(self):
        raw_url = "http://www.elections.state.md.us/elections/2004/election_data/Allegany_County_Democratic_Primary_2004.csv"
        actual = self.fetcher.generate_county_filename(raw_url, '2004-03-02', self.allegany_jurisdiction)
        self.assertEquals("20040302__md__democratic__primary__allegany.csv", actual)

    # 2002 only has two files -- primary and general
    def test_2002_general_filename(self):
        raw_url = "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"
        actual = self.fetcher.generate_2002_filename(raw_url)
        self.assertEquals("20021105__md__general.txt", actual)

    def test_2002_primary_filename(self):
        raw_url = "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"
        actual = self.fetcher.generate_2002_filename(raw_url)
        self.assertEquals("20020910__md__primary.txt", actual)
