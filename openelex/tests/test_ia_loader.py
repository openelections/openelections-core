import datetime
import re
from unittest import skipUnless, TestCase

from openelex.lib.text import ocd_type_id
from openelex.tests import cache_file_exists
from openelex.us.ia.load import (ExcelPrecinctResultLoader,
    ExcelPrecinctPre2010ResultLoader, ExcelPrecinct2010PrimaryResultLoader,
    ExcelPrecinct2010GeneralResultLoader,
    ExcelPrecinct2010GeneralAudubonResultLoader,
    ExcelPrecinct2010GeneralClintonResultLoader, ExcelPrecinct2012ResultLoader,
    ExcelPrecinct2014ResultLoader, LoadResults, PreprocessedResultsLoader)

CACHED_FILE_MISSING_MSG = ("Cached results file does not exist. Try running "
    "the fetch task before running this test")


class LoaderPrepMixin(object):
    def _get_mapping(self, filename):
        return self.loader.datasource.mapping_for_file(filename)

    def _prep_loader_attrs(self, mapping):
        # HACK: set loader's mapping attribute
        # so we can test if loader._file_handle exists.  This
        # usually happens in the loader's run() method.
        self.loader.source = mapping['generated_filename']
        self.loader.election_id = mapping['election']
        self.loader.timestamp = datetime.datetime.now()


class TestLoadResults(TestCase):
    def setUp(self):
        self.loader = LoadResults()

    def test_get_loader(self):
        mapping = {
            'election': u'ia-2000-01-04-special-general',
            'generated_filename': u'20000104__ia__special__general__state_house__53.csv',
            'name': 'Iowa',
            'ocd_id': 'ocd-division/country:us/state:ia',
            'pre_processed_url': 'https://raw.githubusercontent.com/openelections/openelections-data-ia/master/20000104__ia__special__general__state_house__53.csv',
            'raw_url': u'http://sos.iowa.gov/elections/pdf/results/2000s/2000HD53.pdf'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, PreprocessedResultsLoader)

        mapping = {
            'election': u'ia-2008-06-03-primary',
            'generated_filename': u'20080603__ia__primary__wright__precinct.xls',
            'name': u'Wright',
            'ocd_id': u'ocd-division/country:us/state:ia/county:wright',
            'raw_url': u'http://sos.iowa.gov/elections/results/xls/2008/Wright.xls'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinctPre2010ResultLoader)

        mapping = {
            'election': u'ia-2010-06-08-primary',
            'generated_filename': u'20100608__ia__primary__wright__precinct.xls',
            'name': u'Wright',
            'ocd_id': u'ocd-division/country:us/state:ia/county:wright',
            'raw_url': u'http://sos.iowa.gov/elections/results/xls/2010/primary/Wright.xls'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinct2010PrimaryResultLoader)

        mapping = {
            'election': u'ia-2010-11-02-general',
             'generated_filename': u'20101102__ia__general__adair__precinct.xls',
             'name': u'Adair',
             'ocd_id': u'ocd-division/country:us/state:ia/county:adair',
             'raw_url': u'http://sos.iowa.gov/elections/results/xls/2010/general/Adair.xls'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinct2010GeneralResultLoader)

        mapping = {
            'election': u'ia-2012-11-06-general',
            'generated_filename': u'20121106__ia__general__adair__precinct.xls',
            'name': u'Adair',
            'ocd_id': u'ocd-division/country:us/state:ia/county:adair',
            'raw_url': u'http://sos.iowa.gov/elections/results/xls/2012/general/Adair.xls'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinct2012ResultLoader)

        mapping = {
            'election': u'ia-2012-11-06-general',
            'generated_filename': u'20121106__ia__general__adair__precinct.xls',
            'name': u'Adair',
            'ocd_id': u'ocd-division/country:us/state:ia/county:adair',
            'raw_url': u'http://sos.iowa.gov/elections/results/xls/2012/general/Adair.xls'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinct2012ResultLoader)

        mapping = {
            'election': u'ia-2014-06-03-primary',
            'generated_filename': u'20140603__ia__primary__adair__precinct.xlsx',
            'name': u'Adair',
            'ocd_id': u'ocd-division/country:us/state:ia/county:adair',
            'raw_url': u'http://sos.iowa.gov/elections/results/xls/2014/primary/Adair.xlsx'
        }
        loader = self.loader._get_loader(mapping)
        self.assertEqual(loader.__class__, ExcelPrecinct2014ResultLoader)


class TestExcelPrecinctResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinctResultLoader()

    @skipUnless(cache_file_exists('ia',
        '20060606__ia__primary__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_rows(self):
        filename = '20060606__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)
        rows = list(self.loader._rows())
        self.assertEqual(len(rows), 147)


class TestExcelPrecinctPre2010ResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinctPre2010ResultLoader()

    @skipUnless(cache_file_exists('ia',
        '20060606__ia__primary__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results_2006(self):
        filename = '20060606__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        ag_results = [r for r in self.loader._results(mapping)
                      if r.office == "Attorney General"]
        self.assertEqual(len(ag_results), 40)

        result = next(r for r in ag_results
                      if r.reporting_level == 'precinct')
        self.assertEqual(result.source, mapping['generated_filename'])
        self.assertEqual(result.election_id, mapping['election'])
        self.assertEqual(result.state, "IA")
        self.assertEqual(result.election_type, "primary")
        self.assertEqual(result.district, None)
        self.assertEqual(result.party, None)
        self.assertEqual(result.jurisdiction,
            "ADAIR COMMUNITY CENTRE")
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.full_name, "TOM MILLER")
        self.assertEqual(result.votes, 369)

        # There should be some county-level results
        county_ag_results = [r for r in ag_results
            if r.reporting_level == 'county']
        self.assertEqual(len(county_ag_results), 15)
        result = county_ag_results[0]
        result = next(r for r in county_ag_results
                      if r.votes_type == "" and
                      r.full_name == "TOM MILLER")
        self.assertEqual(result.jurisdiction, "Adair")
        self.assertEqual(result.votes, 2298)
        result = next(r for r in county_ag_results
                      if r.votes_type == "absentee" and
                      r.full_name == "TOM MILLER")
        self.assertEqual(result.votes, 524)
        result = next(r for r in county_ag_results
                      if r.votes_type == "provisional" and
                      r.full_name == "TOM MILLER")
        self.assertEqual(result.votes, 0)

        # District attribute should get set on offices with
        # a district
        result = next(r for r in self.loader._results(mapping)
                      if r.office == "State Representative")
        self.assertTrue(re.match(r'\d+', result.district))

    @skipUnless(cache_file_exists('ia',
        '20080603__ia__primary__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results_2008(self):
        filename = '20080603__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        # HACK: set loader's mapping attribute
        # so we can test if loader._file_handle exists.  This
        # usually happens in the loader's run() method.
        self._prep_loader_attrs(mapping)

        senate_results = [r for r in self.loader._results(mapping)
                          if r.office == "United States Senator"]
        self.assertEqual(len(senate_results), 42)
        result = next(r for r in senate_results
                      if r.reporting_level == 'precinct')
        self.assertEqual(result.full_name, "TOM HARKIN")
        self.assertEqual(result.jurisdiction, "ADAIR COMMUNITY CENTRE")

    def test_parse_office(self):
        office_district = [
            "Attorney General",
            "Auditor of State",
            "Governor and Lieutenant Governor",
            "Secretary of Agriculture",
            "Secretary of State",
            "Treasurer of State",
            ("State Representative District 058", "State Representative", "058"),
            ("State Senator District 29", "State Senator", "29"),
            ("United States Representative District 5",
             "United States Representative", "5"),
        ]

        for office in office_district:
            try:
                office_in, expected_office, expected_district = office
            except ValueError:
                office_in = office
                expected_office = office
                expected_district = None

            office_out, district = self.loader._parse_office(office_in)
            self.assertEqual(office_out, expected_office)
            self.assertEqual(district, expected_district)

    def test_parse_result_row(self):
        candidates = [
            "NANCY BOETTGER",
            "",
            "",
            "",
            "",
            "",
            "OverVote",
            "UnderVote",
            "Scattering",
            "Totals",
        ]
        row = [
            "ADAIR COMMUNITY CENTRE",
            377,
            0,
            0,
            0,
            0,
            0,
            0,
            100,
            5,
            482,
        ]
        results = self.loader._parse_result_row(row, candidates,
            county='', county_ocd_id='')
        jurisdiction = row[0]
        result_i = 0
        i = 0
        while i < len(candidates):
            candidate = candidates[i]
            if candidate != "":
                result = results[result_i]
                votes = row[i+1]
                self.assertEqual(result.full_name, candidate)
                self.assertEqual(result.votes, votes)
                self.assertEqual(result.jurisdiction, row[0])
                self.assertEqual(result.reporting_level, 'precinct')
                assert result.ocd_id.endswith(ocd_type_id(jurisdiction))
                result_i += 1

            i += 1

        self.assertEqual(len(results), result_i)

    def test_parse_result_row_pseudo_candidates(self):
        candidates = [
            "NANCY BOETTGER",
            "",
            "",
            "",
            "",
            "",
            "OverVote",
            "UnderVote",
            "Scattering",
            "Totals",
        ]
        row = [
            "ADAIR COMMUNITY CENTRE",
            377,
            0,
            0,
            0,
            0,
            0,
            0,
            100,
            5,
            482,
        ]
        results = self.loader._parse_result_row(row, candidates,
            county='', county_ocd_id='')
        result = next(r for r in results if r.full_name == "OverVote")
        self.assertEqual(result.votes_type, 'over')
        result = next(r for r in results if r.full_name == "UnderVote")
        self.assertEqual(result.votes_type, 'under')

    def test_parse_result_row_absentee(self):
        candidates = [
            "NANCY BOETTGER",
            "",
            "",
            "",
            "",
            "",
            "OverVote",
            "UnderVote",
            "Scattering",
            "Totals",
        ]
        row = [
            'ABSENTEE PRECINCT',
            483,
            0,
            0,
            0,
            0,
            0,
            0,
            249,
            7,
            739,
        ]
        county = 'Adair'
        county_ocd_id = 'ocd-division/country:us/state:ia/county:adair'
        results = self.loader._parse_result_row(row, candidates,
            county=county, county_ocd_id=county_ocd_id)
        result = results[0]
        self.assertEqual(result.reporting_level, 'county')
        self.assertEqual(result.jurisdiction, county)
        self.assertEqual(result.ocd_id, county_ocd_id)
        self.assertEqual(result.votes_type, 'absentee')

    def test_parse_result_row_provisional(self):
        candidates = [
            "NANCY BOETTGER",
            "",
            "",
            "",
            "",
            "",
            "OverVote",
            "UnderVote",
            "Scattering",
            "Totals",
        ]
        row = [
            'PROVISIONAL PRECINCT',
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        county = 'Adair'
        county_ocd_id= 'ocd-division/country:us/state:ia/county:adair'
        results = self.loader._parse_result_row(row, candidates,
            county=county, county_ocd_id=county_ocd_id)
        result = results[0]
        self.assertEqual(result.reporting_level, 'county')
        self.assertEqual(result.jurisdiction, county)
        self.assertEqual(result.ocd_id, county_ocd_id)
        self.assertEqual(result.votes_type, 'provisional')

    def test_votes_type(self):
        # candidate, jurisdiction, expected
        test_vals = (
            ('NANCY BOETTGER', 'ABSENTEE PRECICNT', 'absentee'),
            ('NANCY BOETTGER', 'PROVISIONAL PRECINCT', 'provisional'),
            ('OverVote', 'ADAIR COMMUNITY CENTRE', 'over'),
            ('UnderVote', 'ADAIR COMMUNITY CENTRE', 'under'),
        )

        for candidate, jurisdiction, expected in test_vals:
            votes_type = self.loader._votes_type(candidate, jurisdiction)
            self.assertEqual(votes_type, expected)


class TestExcelPrecinct2010PrimaryResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinct2010PrimaryResultLoader()

    @skipUnless(cache_file_exists('ia',
        '20100608__ia__primary__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results(self):
        filename = '20100608__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        results = self.loader._results(mapping)
        us_rep_dist_5_rep_results = [r for r in results
            if (r.office == "U.S. REPRESENTATIVE" and
                r.district == "5" and
                r.primary_party == "REPUBLICAN")]

        self.assertEqual(len(us_rep_dist_5_rep_results), 35)
        result = us_rep_dist_5_rep_results[0]
        self.assertEqual(result.source, mapping['generated_filename'])
        self.assertEqual(result.election_id, mapping['election'])
        self.assertEqual(result.state, "IA")
        self.assertEqual(result.election_type, "primary")
        self.assertEqual(result.district, "5")
        self.assertEqual(result.party, "REPUBLICAN")
        self.assertEqual(result.jurisdiction,
            "1 NW")
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.full_name, "STEVE KING")
        self.assertEqual(result.votes, 123)

    def test_parse_office_party(self):
        vals = (
            ('U.S. REPRESENTATIVE DISTRICT 5 - DEMOCRATIC PARTY',
             'U.S. REPRESENTATIVE', '5', 'DEMOCRATIC'),
            ('U.S. SENATOR - DEMOCRATIC PARTY',
             'U.S. SENATOR', None, 'DEMOCRATIC'),
            ('GOVERNOR - REPUBLICAN PARTY',
             'GOVERNOR', None, 'REPUBLICAN'),
            ('SECRETARY OF STATE - REPUBLICAN PARTY',
             'SECRETARY OF STATE', None, 'REPUBLICAN'),
            ('AUDITOR OF STATE - REPUBLICAN PARTY',
             'AUDITOR OF STATE', None, 'REPUBLICAN'),
            ('TREASURER OF STATE - DEMOCRATIC PARTY',
             'TREASURER OF STATE', None, 'DEMOCRATIC'),
            ('SECRETARY OF AGRICULTURE - REPUBLICAN PARTY',
             'SECRETARY OF AGRICULTURE', None, 'REPUBLICAN'),
            ('ATTORNEY GENERAL - REPUBLICAN PARTY',
             'ATTORNEY GENERAL', None, 'REPUBLICAN'),
            ('STATE SENATOR DISTRICT 29 - DEMOCRATIC PARTY',
             'STATE SENATOR', '29', 'DEMOCRATIC'),
            ('STATE REPRESENTATIVE DISTRICT 58 - REPUBLICAN PARTY',
             'STATE REPRESENTATIVE', '58', 'REPUBLICAN'),
        )

        for raw, office_expected, district_expected, party_expected in vals:
            office, district, party = self.loader._parse_office_party(raw)
            self.assertEqual(office, office_expected)
            self.assertEqual(district, district_expected)
            self.assertEqual(party, party_expected)

    def test_parse_candidates(self):
        row = [
            'Race',
            'County',
            'Precinct',
            'ROXANNE CONLIN',
            'THOMAS L. FIEGEN',
            'BOB KRAUSE',
            'WRITE-IN',
            'Over Votes',
            'Under Votes',
            'Precinct Totals',
            'Final Data?',
        ]
        candidates = self.loader._parse_candidates(row)
        self.assertEqual(candidates[0], 'ROXANNE CONLIN')
        self.assertEqual(candidates[-1], 'Precinct Totals')

    def test_parse_result_row(self):
        candidates = [
            'ROXANNE CONLIN',
            'THOMAS L. FIEGEN',
            'BOB KRAUSE',
            'WRITE-IN',
            'Over Votes',
            'Under Votes',
            'Precinct Totals',
        ]
        row = [
            'U.S. SENATOR - DEMOCRATIC PARTY',
            'Adair',
            '1 NW',
            30,
            6,
            4,
            0,
            0,
            1,
            41,
            'Y',
        ]
        county = "Adair"
        county_ocd_id = "ocd-division/country:us/state:ia/county:adair"

        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.jurisdiction, row[2])
        self.assertEqual(result.full_name, candidates[0])
        self.assertEqual(result.votes, row[3])

    def test_parse_result_row_grand_totals(self):
        candidates = [
            'ROXANNE CONLIN',
            'THOMAS L. FIEGEN',
            'BOB KRAUSE',
            'WRITE-IN',
            'Over Votes',
            'Under Votes',
            'Precinct Totals',
        ]

        row = [
            'Grand Totals',
            '',
            '',
            132,
            17,
            25,
            0,
            1,
            8,
            183,
            '',
        ]
        county = "Adair"
        county_ocd_id = "ocd-division/country:us/state:ia/county:adair"
        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.jurisdiction, "Adair")
        self.assertEqual(result.full_name, candidates[0])
        self.assertEqual(result.votes, row[3])

    def test_parse_result_row_grand_absentee(self):
        candidates = [
            'ROXANNE CONLIN',
            'THOMAS L. FIEGEN',
            'BOB KRAUSE',
            'WRITE-IN',
            'Over Votes',
            'Under Votes',
            'Precinct Totals',
        ]

        row = [
            'U.S. SENATOR - DEMOCRATIC PARTY',
            'Adair',
            'ABSENTEE',
            16,
            0,
            8,
            0,
            1,
            1,
            26,
            'Y',
        ]

        county = "Adair"
        county_ocd_id = "ocd-division/country:us/state:ia/county:adair"
        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.jurisdiction, "Adair")
        self.assertEqual(result.full_name, candidates[0])
        self.assertEqual(result.votes, row[3])
        self.assertEqual(result.votes_type, 'absentee')

    def test_votes_type(self):
        # candidate, jurisdiction, expected
        test_vals = (
            ('ROXANNE CONLIN', 'ABSENTEE', 'absentee'),
            ('Over Votes', '1 NW', 'over'),
            ('Under Votes', '1 NW', 'under'),
        )

        for candidate, jurisdiction, expected in test_vals:
            votes_type = self.loader._votes_type(candidate, jurisdiction)
            self.assertEqual(votes_type, expected)


class TestExcelPrecinct2010GeneralResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinct2010GeneralResultLoader()

    def test_parse_result_row_skip(self):
        rows = [
            [u'Arcadia Precinct ', u'Race Statistics ', u'Number of Precincts ', u'Polling', 1.0],
            [u'Arcadia Precinct ', u'U.S. Senator ', u'Times Counted ', u'Total', 269.0],
            [u'Ewoldt Precinct ', u'Sup Crt Judge Ternus ', u'Yes ', u'Polling ', 157.0]
        ]
        county = None
        county_ocd_id = None
        for row in rows:
            self.assertEqual(self.loader._parse_result_row(row, county,
                county_ocd_id), None)

    def test_parse_result_row(self):
        county = "Carroll"
        county_ocd_id='ocd-division/country:us/state:ia/county:carroll'
        row = [u'Carroll Ward Three & S1/2 Maple River Twp. ', u'Secretary of State ', u'Michael A. Mauro ', u'Total ', 431.0]
        expected_ocd_id = county_ocd_id + '/' + ocd_type_id(row[0].strip())

        results = self.loader._parse_result_row(row, county, county_ocd_id)
        self.assertEqual(len(results), 1)

        result = results[0]
        self.assertEqual(result.jurisdiction, row[0].strip())
        self.assertEqual(result.ocd_id, expected_ocd_id)
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.office, row[1].strip())
        self.assertEqual(result.full_name, row[2].strip())
        self.assertEqual(result.votes, row[4])
        self.assertEqual(result.votes_type, '')

    def test_parse_result_row_multiple(self):
        """
        Test parsing a result row when the absentee and election day votes
        are in different columns instead of different rows.

        """
        county = "Wapello"
        county_ocd_id='ocd-division/country:us/state:ia/county:wapello'
        row = [u'Precinct 1', u'US Senator', u'Chuck Grassley', 230.0, 127.0, 357.0]
        expected_ocd_id = county_ocd_id + '/' + ocd_type_id(row[0].strip())

        results = self.loader._parse_result_row(row, county, county_ocd_id)
        self.assertEqual(len(results), 3)

        for result in results:
            self.assertEqual(result.jurisdiction, row[0].strip())
            self.assertEqual(result.ocd_id, expected_ocd_id)
            self.assertEqual(result.reporting_level, 'precinct')
            self.assertEqual(result.office, row[1].strip())
            self.assertEqual(result.full_name, row[2].strip())

        result = next(r for r in results if r.votes_type == 'polling')
        self.assertEqual(result.votes, row[3])

        result = next(r for r in results if r.votes_type == 'absentee')
        self.assertEqual(result.votes, row[4])

        result = next(r for r in results if r.votes_type == '')
        self.assertEqual(result.votes, row[5])


class TestExcelPrecinct2010GeneralAudubonResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinct2010GeneralAudubonResultLoader()

    def test_clean_jurisdiction_cell(self):
        test_data = [
          ('Absentee     One', 'Absentee One'),
        ]
        for val, expected in test_data:
            self.assertEqual(self.loader._clean_jurisdiction_cell(val),
                expected)

    def test_parse_jurisdictions(self):
        row = [u'Candidates', u'Party', u'One', u'Absentee     One', u'Total Precinct One', u'Two', u'Abs Two', u'Total Precinct Two', u'Three', u'Abs   Three', u'Total Precinct Three', u'Four', u'Abs     Four', u'Total Precinct Four', u'Special & Abs', u'Total', '', '']
        expected = [
            "One",
            "Absentee One",
            "Total Precinct One",
            "Two",
            "Abs Two",
            "Total Precinct Two",
            "Three",
            "Abs Three",
            "Total Precinct Three",
            "Four",
            "Abs Four",
            "Total Precinct Four",
            "Special & Abs",
            "Total",
        ]
        jurisdictions = self.loader._parse_jurisdictions(row)
        self.assertEqual(jurisdictions, expected)

    def test_parse_office(self):
        # Cell value, expected office, expected district
        test_data = [
            ('United States Senator', "United States Senator", None),
            ('United States Representative - Dist 5', "United States Representative", "5"),
            ('Board of Supervisors', None, None),
            ('Secretary of State', 'Secretary of State', None),
            ('Auditor of State', 'Auditor of State', None),
            ('Treasurer of State', 'Treasurer of State', None),
            ('Secretary of Agriculture', 'Secretary of Agriculture', None),
            ('Attorney General', 'Attorney General', None),
            ('State Senator District 29', 'State Senator', '29'),
            ('State Representative District 58', 'State Representative', '58'),
        ]
        for cell, expected_office, expected_district in test_data:
            office, district = self.loader._parse_office(cell)
            self.assertEqual(office, expected_office)
            self.assertEqual(district, expected_district)

    def test_parse_result_row(self):
        county = "Audubon"
        county_ocd_id = 'ocd-division/country:us/state:ia/county:audubon'
        row = [u'Steve King', u'REP', 371.0, 205.0, 576.0, 236.0, 98.0, 334.0, 127.0, 22.0, 149.0, 344.0, 124.0, 468.0, 449.0, 1527.0, 1527.0, '']
        jurisdictions = [
            "One",
            "Absentee One",
            "Total Precinct One",
            "Two",
            "Abs Two",
            "Total Precinct Two",
            "Three",
            "Abs Three",
            "Total Precinct Three",
            "Four",
            "Abs Four",
            "Total Precinct Four",
            "Special & Abs",
            "Total",
        ]
        results = self.loader._parse_result_row(row, jurisdictions, county,
            county_ocd_id)
        self.assertEqual(len(results), len(jurisdictions))
        for result, votes, jurisdiction in zip(results, row[2:-1], jurisdictions):
            reporting_level = 'precinct'
            votes_type = ''
            clean_jurisdiction = jurisdiction
            if jurisdiction in ("Total", "Special & Abs"):
                clean_jurisdiction = county
                reporting_level = 'county'

            if "Abs" in jurisdiction:
                votes_type = 'absentee'
            elif "Total" in jurisdiction:
                votes_type = ''
            else:
                votes_type = 'election_day'

            self.assertEqual(result.full_name, row[0].strip())
            self.assertEqual(result.party, row[1].strip())
            self.assertEqual(result.jurisdiction, clean_jurisdiction)
            self.assertEqual(result.reporting_level, reporting_level)
            self.assertEqual(result.votes, votes)
            self.assertEqual(result.votes_type, votes_type)


class TestExcelPrecinct2010GeneralClintonResultLoader(LoaderPrepMixin,
        TestCase):

    def setUp(self):
        self.loader = ExcelPrecinct2010GeneralClintonResultLoader() 
    def test_parse_office(self):
        row = [u'US SENATOR', '', '', '', '']
        office, district = self.loader._parse_office(row)
        self.assertEqual(office, 'US SENATOR')
        self.assertEqual(district, None)

        row = [u'US REP DIST 1', '', '', '', '']
        office, district = self.loader._parse_office(row)
        self.assertEqual(office, 'US REP')
        self.assertEqual(district, '1')

    def test_parse_result_row(self):
        row = [u'Roxanne Conlin (DEM).  .  .  .  .  .  .', 371.0, 43.75, 190.0, 181.0]
        results = self.loader._parse_result_row(row)
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertEqual(result.full_name, "Roxanne Conlin")
            self.assertEqual(result.party, "DEM")
        self.assertEqual(results[0].votes, row[1])
        self.assertEqual(results[0].votes_type, '')
        self.assertEqual(results[1].votes, row[3])
        self.assertEqual(results[1].votes_type, 'absentee')
        self.assertEqual(results[2].votes, row[4])
        self.assertEqual(results[2].votes_type, 'election_day')

    def test_parse_candidate(self):
        test_data = [
          ('Roxanne Conlin (DEM).  .  .  .  .  .  .', "Roxanne Conlin", "DEM"),
          ('WRITE-IN.  .  .  .  .  .  .  .  .  .  .', "WRITE-IN", None),
          ('Tod R. Bowman (DEM) .  .  .  .  .  .  .', "Tod R. Bowman", "DEM")
        ]
        for cell, expected_candidate, expected_party in test_data:
            candidate, party = self.loader._parse_candidate(cell)
            self.assertEqual(candidate, expected_candidate)
            self.assertEqual(party, expected_party)

    @skipUnless(cache_file_exists('ia',
        '20101102__ia__general__clinton__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results(self):
        filename = '20101102__ia__general__clinton__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        results = self.loader._results(mapping)
        filtered_results = [r for r in results 
            if r.office == 'ST SEN' and
            r.district == '13' and
            r.jurisdiction == '0008 CLINTON 1-2']
        self.assertEqual(len(filtered_results), 15)

        result = filtered_results[0]
        self.assertEqual(result.full_name, "Tod R. Bowman")
        self.assertEqual(result.party, "DEM")
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.votes, 544)
        self.assertEqual(result.votes_type, "")

        result = filtered_results[-1]
        self.assertEqual(result.full_name, "Under Votes")
        self.assertEqual(result.party, None)
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.votes, 4)
        self.assertEqual(result.votes_type, "election_day")


class TestExcelPrecinct2012Loader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinct2012ResultLoader()

    @skipUnless(cache_file_exists('ia',
        '20120605__ia__primary__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results_primary(self):
        filename = '20120605__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        results = self.loader._results(mapping)
        state_senate_district_10_rep_results = [r for r in results
            if (r.office == "State Senator" and
                r.district == "10" and
                r.primary_party == "R")]
        self.assertEqual(len(state_senate_district_10_rep_results), 42)

        result = next(r for r in state_senate_district_10_rep_results
                      if r.jurisdiction == "District 1NW")
        self.assertEqual(result.source, mapping['generated_filename'])
        self.assertEqual(result.election_id, mapping['election'])
        self.assertEqual(result.state, "IA")
        self.assertEqual(result.election_type, "primary")
        self.assertEqual(result.district, "10")
        self.assertEqual(result.party, "R")
        self.assertEqual(result.jurisdiction, "District 1NW")
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.full_name, "Jake Chapman")
        self.assertEqual(result.votes, 39)

    @skipUnless(cache_file_exists('ia',
        '20121106__ia__general__adair__precinct.xls'), CACHED_FILE_MISSING_MSG)
    def test_results_general(self):
        filename = '20121106__ia__general__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        results = self.loader._results(mapping)
        state_rep_district_20_results = [r for r in results
            if (r.office == "State Representative" and r.district == "20")]
        precinct_2ne_results = [r for r in state_rep_district_20_results
            if r.jurisdiction == "2NE"]
        self.assertEqual(len(precinct_2ne_results), 18)
        absentee_results = [r for r in state_rep_district_20_results if r.votes_type == 'absentee']
        self.assertEqual(len(absentee_results), 36)
        county_results = [r for r in state_rep_district_20_results if r.reporting_level == 'county']
        self.assertEqual(len(county_results), 18)

        result = state_rep_district_20_results[0]
        self.assertEqual(result.full_name, "Clel Baudler")
        self.assertEqual(result.party, "Republican")
        self.assertEqual(result.primary_party, None)
        self.assertEqual(result.state, "IA")
        self.assertEqual(result.election_type, 'general')
        self.assertEqual(result.office, "State Representative")
        self.assertEqual(result.district, "20")
        self.assertEqual(result.reporting_level, 'precinct')
        self.assertEqual(result.votes, 400)
        self.assertEqual(result.votes_type, 'election_day')

        result = state_rep_district_20_results[-1]
        self.assertEqual(result.full_name, "Total")
        self.assertEqual(result.party, None)
        self.assertEqual(result.primary_party, None)
        self.assertEqual(result.state, "IA")
        self.assertEqual(result.election_type, 'general')
        self.assertEqual(result.office, "State Representative")
        self.assertEqual(result.district, "20")
        self.assertEqual(result.reporting_level, 'county')
        self.assertEqual(result.votes, 4020)
        self.assertEqual(result.votes_type, '')


    def test_page_header_row(self):
        row = ['', '', '', u'ADAIR COUNTY ELECTION CANVASS SUMMARY\n2012 IOWA PRIMARY ELECTION', '', '', '', '', '', '', ''];
        self.assertEqual(self.loader._page_header_row(row), True)

        row = ['', '', '', '', u'ADAIR COUNTY ELECTION CANVASS SUMMARY\n2012 IOWA GENERAL ELECTION', '', '', '', '', '', '', '', '', '', '', '', '', '']
        self.assertEqual(self.loader._page_header_row(row), True)

    def test_empty_row(self):
        row = ['', '', '', '', '', u'President/Vice President', '', '', '', '', '', '', '', '', '', '', '', u'\n\n\nTotal']
        self.assertEqual(self.loader._empty_row(row), False)

        row = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
        self.assertEqual(self.loader._empty_row(row), True)

    def test_parse_office_row(self):
        row = ['', '', '', '', '', u'U.S. House of Representatives \nDistrict 3 - R', '', '', '', u'\n\n\nTotal', '']
        office, district, primary_party = self.loader._parse_office_row(row)
        self.assertEqual(office, "U.S. House of Representatives")
        self.assertEqual(district, "3")
        self.assertEqual(primary_party, "R")

        row = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
        office, district, primary_party = self.loader._parse_office_row(row)
        self.assertEqual(office, None)
        self.assertEqual(district, None)
        self.assertEqual(primary_party, None)

        row = ['', '', '', '', '', u'President/Vice President', '', '', '', '', '', '', '', '', '', '', '', u'\n\n\nTotal']
        office, district, primary_party = self.loader._parse_office_row(row)
        self.assertEqual(office, "President/Vice President")
        self.assertEqual(district, None)
        self.assertEqual(primary_party, None)

    def test_parse_candidates_row(self):
        row = [u'Precinct', '', '', '', '', '', '', '', '', '', '']
        candidates = self.loader._parse_candidates_row(row)
        self.assertEqual(candidates, None)

        row = ['', '', '', '', '', u' Jake Chapman', u' Matthew T. Mardesen', u'Write-In', u' Under Votes', u' Over Votes', '']
        candidates = self.loader._parse_candidates_row(row)
        self.assertEqual(len(candidates), 5)
        candidate, party = candidates[0]
        self.assertEqual(candidate, "Jake Chapman")
        self.assertEqual(party, None)
        candidate, party = candidates[-1]
        self.assertEqual(candidate, "Over Votes")
        self.assertEqual(party, None)

        row = [u'Precinct', '', '', '', '', u' Clel Baudler\nRepublican', u' Greg Nepstad\nDemocratic', u'Write-In', u' Under Votes', u' Over Votes', '', '', '', '', '', '', '', '']
        candidates = self.loader._parse_candidates_row(row)
        self.assertEqual(len(candidates), 5)
        candidate, party = candidates[0]
        self.assertEqual(candidate, "Clel Baudler")
        self.assertEqual(party, "Republican")
        candidate, party = candidates[-1]
        self.assertEqual(candidate, "Over Votes")
        self.assertEqual(party, None)

    def test_parse_jurisdiction(self):
        # Primary election examples
        prev_reporting_level = None
        row = [u'Absentee', '', '', '', '', 8.0, 1.0, 1.0, 0.0, 10.0, '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'county')

        prev_reporting_level = 'precinct'
        row = [u'District 1NW', '', '', '', '', 10.0, 0.0, 2.0, 0.0, 12.0, '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'precinct')

        prev_reporting_level = 'precinct'
        row = [u'            Total ', '', '', '', '', 56.0, 1.0, 4.0, 0.0, 61.0, '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0].strip())
        self.assertEqual(level, 'county')

        # General election examples
        prev_reporting_level = 'precinct'
        row = [u'3SW', '', u'Election Day', '', '', 411.0, 17.0, 69.0, 0.0, 497.0, '', '', '', '', '', '', '', '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'precinct')

        prev_reporting_level = 'precinct'
        row = ['', '', u'Election Day', '', '', 411.0, 17.0, 69.0, 0.0, 497.0, '', '', '', '', '', '', '', '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'county')

        prev_reporting_level = 'precinct'
        row = ['', '', u'Absentee', '', '', 251.0, 3.0, 144.0, 0.0, 398.0, '', '', '', '', '', '', '', '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'precinct')

        prev_reporting_level = 'county'
        row = ['', '', u'Absentee', '', '', 1103.0, 17.0, 570.0, 0.0, 1690.0, '', '', '', '', '', '', '', '']
        jurisdiction, level = self.loader._parse_jurisdiction(row,
            prev_reporting_level)
        self.assertEqual(jurisdiction, row[0])
        self.assertEqual(level, 'county')

    def test_parse_result_row_primary(self):
        county = "Adair"
        county_ocd_id = 'ocd-division/country:us/state:ia/county:adair'

        common_kwargs = {
            'reporting_level': 'precinct',
            'jurisdiction': 'District 1NW',
        }
        candidates = [
            ('Jake Chapman', None),
            ('Matthew T. Mardesen', None),
            ('Write-In', None),
            ('Under Votes', None),
            ('Over Votes', None),
            ('Total', None)
        ]
        row = [u'District 1NW', '', '', '', '', 39.0, 19.0, 0.0, 0.0, 0.0, 58.0]
        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id, **common_kwargs)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.full_name, candidates[0][0])
        self.assertEqual(result.votes, row[5])
        self.assertEqual(result.reporting_level, "precinct")
        self.assertEqual(result.jurisdiction, row[0])
        result = results[-1]
        self.assertEqual(result.full_name, candidates[-1][0])
        self.assertEqual(result.votes, row[-1])

    def test_fix_row(self):
        row = [u'District 1NW', '', '', '', '', 39.0, 19.0, 0.0, 0.0, 0.0, 58.0]
        self.assertEqual(self.loader._fix_row(row), row)

        row = [u'ABSENTEE 1', '', '', '', '', 26.0, 35.0, 0.0, 35.0, 1.0, 12.0, '', '', 0.0, '', u'ABSENTEE 1', '', '', '', '', 0.0, '', 0.0, '', '', 109.0]
        fixed = row[0:14] + row[16:]
        self.assertEqual(self.loader._fix_row(row), fixed)

    def test_parse_results_row_general(self):
        county = "Adair"
        county_ocd_id = 'ocd-division/country:us/state:ia/county:adair'

        candidates = [
            ('Tom Latham', 'Republican'),
            ('Leonard Boswell', 'Democratic'),
            ('David Rosenfeld', 'Socialist Workers Party'),
            ('Scott G. Batcher', 'Nominated by Petition'),
            ('Write-In', None),
            ('Under Votes', None),
            ('Over Votes', None),
            ('Total', None),
        ]
        common_kwargs = {
            'jurisdiction': '1NW',
            'reporting_level': 'precinct',
        }
        row = [u'1NW', '', u'Election Day', '', '', 366.0, 140.0, 13.0, 13.0, 0.0, 11.0, 0.0, 543.0, '', '', '', '', '']
        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id, **common_kwargs)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.full_name, candidates[0][0])
        self.assertEqual(result.votes, row[5])
        self.assertEqual(result.reporting_level, "precinct")
        self.assertEqual(result.jurisdiction, row[0])
        result = results[-1]
        self.assertEqual(result.full_name, candidates[-1][0])
        self.assertEqual(result.votes, row[-6])

        row = ['', '', u'Total', '', '', 546.0, 221.0, 17.0, 22.0, 0.0, 23.0, 1.0, 830.0, '', '', '', '', '']
        results = self.loader._parse_result_row(row, candidates, county,
            county_ocd_id, **common_kwargs)
        self.assertEqual(len(results), len(candidates))
        result = results[0]
        self.assertEqual(result.full_name, candidates[0][0])
        self.assertEqual(result.votes, row[5])
        self.assertEqual(result.reporting_level, "precinct")
        self.assertEqual(result.jurisdiction, common_kwargs['jurisdiction'])
        result = results[-1]
        self.assertEqual(result.full_name, candidates[-1][0])
        self.assertEqual(result.votes, row[-6])

    def test_is_last_contest_row(self):
        row = [u'            Total ', '', '', '', '', 622.0, 3.0, 144.0, 0.0, 769.0, '', '', '', '', '', '', '', '', '', '', '']
        reporting_level = 'county'
        self.assertEqual(self.loader._is_last_contest_result(row,
            reporting_level), True)

        row = [u'Lorimor City Hall', '', '', '', '', 44.0, 0.0, 1.0, 0.0, 45.0, '', '', '', '', '', '', '', '', '', '', '']
        reporting_level = 'precinct'
        self.assertEqual(self.loader._is_last_contest_result(row,
            reporting_level), False)

        row = ['', '', u'Total', '', '', 2192.0, 2335.0, 2449.0, 2352.0, 26.0, 804623.0, 1.0, 813978.0, '', '', '', '', '']
        reporting_level = 'county'
        self.assertEqual(self.loader._is_last_contest_result(row,
            reporting_level), True)

        row = ['', '', u'Total', '', '', 2192.0, 2335.0, 2449.0, 2352.0, 26.0, 804623.0, 1.0, 813978.0, '', '', '', '', '']
        reporting_level = 'precinct'
        self.assertEqual(self.loader._is_last_contest_result(row,
            reporting_level), False)

        row = ['', '', u'Absentee', '', '', 878.0, 979.0, 993.0, 985.0, 15.0, 1523.0, 1.0, 5374.0, '', '', '', '', '']
        reporting_level = 'county'
        self.assertEqual(self.loader._is_last_contest_result(row,
            reporting_level), False)

    def test_parse_votes_type(self):
        row = [u'Absentee', '', '', '', '', 8.0, 1.0, 1.0, 0.0, 10.0, '']
        self.assertEqual(self.loader._parse_votes_type(row), 'absentee')

        row = [u'District 1NW', '', '', '', '', 10.0, 0.0, 2.0, 0.0, 12.0, '']
        self.assertEqual(self.loader._parse_votes_type(row), '')

        row = [u'            Total ', '', '', '', '', 56.0, 1.0, 4.0, 0.0, 61.0, '']
        self.assertEqual(self.loader._parse_votes_type(row), '')

        row = [u'3SW', '', u'Election Day', '', '', 411.0, 17.0, 69.0, 0.0, 497.0, '', '', '', '', '', '', '', '']
        self.assertEqual(self.loader._parse_votes_type(row), 'election_day')

        row = ['', '', u'Absentee', '', '', 251.0, 3.0, 144.0, 0.0, 398.0, '', '', '', '', '', '', '', '']
        self.assertEqual(self.loader._parse_votes_type(row), 'absentee')

        row = ['', '', u'Total', '', '', 2659.0, 1219.0, 5.0, 133.0, 4.0, 4020.0, '', '', '', '', '', '', '']
        self.assertEqual(self.loader._parse_votes_type(row), '')


class TestExcellPrecinct2014Loader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinct2014ResultLoader()

    def test_parse_jurisdictions(self):
        row = [u'RaceTitle', u' CandidateName', u' PoliticalPartyName', '', u' Adair-1 NW Polling', u' Adair-2 NE Polling', u' Adair-3 SW Polling', u' Adair-4 SE Polling', u' Adair-5 GF Polling', u' Adair-Absentee Absentee', u' Adair Total']
        expected = [
            ('Adair-1 NW Polling', 'precinct', 'election_day'),
            ('Adair-2 NE Polling', 'precinct', 'election_day'),
            ('Adair-3 SW Polling', 'precinct', 'election_day'),
            ('Adair-4 SE Polling', 'precinct', 'election_day'),
            ('Adair-5 GF Polling', 'precinct', 'election_day'),
            ('Adair-Absentee Absentee', 'county', 'absentee'),
            ('Adair Total', 'county', 'total'),
        ]
        jurisdictions = self.loader._parse_jurisdictions(row)
        self.assertEqual(jurisdictions, expected)

        row = [u'RaceTitle', u' CandidateName', u' PoliticalPartyName', u' Clay-Douglas/Peterson Absentee', u' Clay-Douglas/Peterson Polling', u' Clay-Douglas/Peterson Total', u' Clay-Garfield/Herdland Absentee', u' Clay-Garfield/Herdland Polling', u' Clay-Garfield/Herdland Total', u' Clay-Lake/Freeman Absentee', u' Clay-Lake/Freeman Polling', u' Clay-Lake/Freeman Total', u' Clay-Lincoln/Clay Absentee', u' Clay-Lincoln/Clay Polling', u' Clay-Lincoln/Clay Total', u' Clay-Logan/Gillett Grove Absentee', u' Clay-Logan/Gillett Grove Polling', u' Clay-Logan/Gillett Grove Total', u' Clay-Spencer Ward 1 Absentee', u' Clay-Spencer Ward 1 Polling', u' Clay-Spencer Ward 1 Total', u' Clay-Spencer Ward 2 Absentee', u' Clay-Spencer Ward 2 Polling', u' Clay-Spencer Ward 2 Total', u' Clay-Spencer Ward 3 Absentee', u' Clay-Spencer Ward 3 Polling', u' Clay-Spencer Ward 3 Total', u' Clay-Spencer Ward 4 Absentee', u' Clay-Spencer Ward 4 Polling', u' Clay-Spencer Ward 4 Total', u' Clay-Spencer Ward 5 Absentee', u' Clay-Spencer Ward 5 Polling', u' Clay-Spencer Ward 5 Total', u' Clay-Summit/Riverton/Sioux/Meadow Absentee', u' Clay-Summit/Riverton/Sioux/Meadow Polling', u' Clay-Summit/Riverton/Sioux/Meadow Total', u' Clay-Waterford/Lone Tree Absentee', u' Clay-Waterford/Lone Tree Polling', u' Clay-Waterford/Lone Tree Total', u' Clay Total']


        expected = [
            ('Clay-Douglas/Peterson Absentee', 'precinct', 'absentee'),
            ('Clay-Douglas/Peterson Polling', 'precinct', 'election_day'),
            ('Clay-Douglas/Peterson Total', 'precinct', 'total'),
            ('Clay-Garfield/Herdland Absentee', 'precinct', 'absentee'),
            ('Clay-Garfield/Herdland Polling', 'precinct', 'election_day'),
            ('Clay-Garfield/Herdland Total', 'precinct', 'total'),
            ('Clay-Lake/Freeman Absentee', 'precinct', 'absentee'),
            ('Clay-Lake/Freeman Polling', 'precinct', 'election_day'),
            ('Clay-Lake/Freeman Total', 'precinct', 'total'),
            ('Clay-Lincoln/Clay Absentee', 'precinct', 'absentee'),
            ('Clay-Lincoln/Clay Polling', 'precinct', 'election_day'),
            ('Clay-Lincoln/Clay Total', 'precinct', 'total'),
            ('Clay-Logan/Gillett Grove Absentee', 'precinct', 'absentee'),
            ('Clay-Logan/Gillett Grove Polling', 'precinct', 'election_day'),
            ('Clay-Logan/Gillett Grove Total', 'precinct', 'total'),
            ('Clay-Spencer Ward 1 Absentee', 'precinct', 'absentee'),
            ('Clay-Spencer Ward 1 Polling', 'precinct', 'election_day'),
            ('Clay-Spencer Ward 1 Total', 'precinct', 'total'),
            ('Clay-Spencer Ward 2 Absentee', 'precinct', 'absentee'),
            ('Clay-Spencer Ward 2 Polling', 'precinct', 'election_day'),
            ('Clay-Spencer Ward 2 Total', 'precinct', 'total'),
            ('Clay-Spencer Ward 3 Absentee', 'precinct', 'absentee'),
            ('Clay-Spencer Ward 3 Polling', 'precinct', 'election_day'),
            ('Clay-Spencer Ward 3 Total', 'precinct', 'total'),
            ('Clay-Spencer Ward 4 Absentee', 'precinct', 'absentee'),
            ('Clay-Spencer Ward 4 Polling', 'precinct', 'election_day'),
            ('Clay-Spencer Ward 4 Total', 'precinct', 'total'),
            ('Clay-Spencer Ward 5 Absentee', 'precinct', 'absentee'),
            ('Clay-Spencer Ward 5 Polling', 'precinct', 'election_day'),
            ('Clay-Spencer Ward 5 Total', 'precinct', 'total'),
            ('Clay-Summit/Riverton/Sioux/Meadow Absentee', 'precinct', 'absentee'),
            ('Clay-Summit/Riverton/Sioux/Meadow Polling', 'precinct',
             'election_day'),
            ('Clay-Summit/Riverton/Sioux/Meadow Total', 'precinct', 'total'),
            ('Clay-Waterford/Lone Tree Absentee', 'precinct', 'absentee'),
            ('Clay-Waterford/Lone Tree Polling', 'precinct', 'election_day'),
            ('Clay-Waterford/Lone Tree Total', 'precinct', 'total'),
            ('Clay Total', 'county', 'total'),
        ]

    def test_parse_jurisdiction(self):
        test_data = [
            ('Adair-1 NW Polling', 'precinct', 'election_day'),
            ('Adair-Absentee Absentee', 'county', 'absentee'),
            ('Adair Total', 'county', 'total'),
            ('Clay-Waterford/Lone Tree Absentee', 'precinct', 'absentee'),
            ('Clay-Waterford/Lone Tree Polling', 'precinct', 'election_day'),
            ('Clay-Waterford/Lone Tree Total', 'precinct', 'total'),
            ('Clay Total', 'county', 'total'),
        ]
        for expected in test_data:
            val = expected[0]
            result = self.loader._parse_jurisdiction(val)
            self.assertEqual(result, expected)

    def test_parse_result_row(self):
        county = "Adair"
        county_ocd_id = 'ocd-division/country:us/state:ia/county:adair'
        jurisdictions = [
            ('Adair-1 NW Polling', 'precinct', 'election_day'),
            ('Adair-2 NE Polling', 'precinct', 'election_day'),
            ('Adair-3 SW Polling', 'precinct', 'election_day'),
            ('Adair-4 SE Polling', 'precinct', 'election_day'),
            ('Adair-5 GF Polling', 'precinct', 'election_day'),
            ('Adair-Absentee Absentee', 'county', 'absentee'),
            ('Adair Total', 'county', 'total'),
        ]
        common_kwargs = {
          'election_type': 'primary',
        }

        row = [u'U.S. Senator - Rep', u' Sam Clovis', u' Republican Party', '', 15.0, 13.0, 13.0, 3.0, 10.0, 8.0, 62.0]
        results = self.loader._parse_result_row(row, jurisdictions,
            county, county_ocd_id, **common_kwargs)
        self._test_result_common_fields(row, jurisdictions, results)
        self._test_result_at_index(row, jurisdictions, results, 0)
        self._test_result_at_index(row, jurisdictions, results, -2)
        self._test_result_at_index(row, jurisdictions, results, -1)

        row = [u'U.S. Rep. Dist. 3 - Rep', u' Robert Cramer', u' Republican Party', '', 26.0, 15.0, 14.0, 14.0, 20.0, 20.0, 109.0]
        results = self.loader._parse_result_row(row, jurisdictions,
            county, county_ocd_id, **common_kwargs)
        self._test_result_common_fields(row, jurisdictions, results, '3')

    def _test_result_common_fields(self, row, jurisdictions, results,
            district=None):
        """Test that fields common to all results from a row are correct"""
        self.assertEqual(len(results), len(jurisdictions))
        full_name = row[1].strip()
        party = row[2].strip()
        primary_party = party[0:3]
        for result in results:
            self.assertEqual(result.full_name, full_name)
            self.assertEqual(result.party, party)
            self.assertEqual(result.primary_party, primary_party)
            self.assertEqual(result.district, district)

    def _test_result_at_index(self, row, jurisdictions, results, i):
        """Test that a result at an index is well formed"""
        result = results[i]
        vote_index = i if i < 0 else i + 4
        votes_type = ('' if jurisdictions[i][2] == 'total'
                      else jurisdictions[i][2])
        self.assertEqual(result.jurisdiction, jurisdictions[i][0])
        self.assertEqual(result.reporting_level, jurisdictions[i][1])
        self.assertEqual(result.votes_type, votes_type)
        self.assertEqual(result.votes, row[vote_index])
