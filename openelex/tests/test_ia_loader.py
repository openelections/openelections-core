import datetime
import re
from unittest import TestCase

from openelex.lib.text import ocd_type_id
from openelex.us.ia.load import (ExcelPrecinctResultLoader,
    ExcelPrecinctPre2010ResultLoader, ExcelPrecinct2010PrimaryResultLoader,
    ExcelPrecinct2010GeneralResultLoader,
    LoadResults, PreprocessedResultsLoader)


class LoaderPrepMixin(object):
    def _get_mapping(self, filename):
        yr = filename[:4]
        mappings = self.loader.datasource.mappings(yr)
        return next(m for m in mappings if m['generated_filename'] == filename)

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
        # BOOKMARK


class TestExcelPrecinctResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinctResultLoader()

    def test_rows(self):
        filename = '20060606__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)
        rows = list(self.loader._rows())
        self.assertEqual(len(rows), 147)


class TestExcelPrecinctPre2010ResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinctPre2010ResultLoader()

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
        county_ocd_id= 'ocd-division/country:us/state:ia/county:adair'
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
