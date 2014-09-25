import datetime
import re
from unittest import TestCase

from openelex.lib.text import ocd_type_id
from openelex.us.ia.load import (ExcelPrecinctResultLoader,
    ExcelPrecinctPre2010ResultLoader, ExcelPrecinctPost2010ResultLoader)


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
        self.assertEqual(len(ag_results), 30)
        
        result = ag_results[0]
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

        # There should be a county-level result
        county_ag_results = [r for r in ag_results
            if r.reporting_level == 'county']
        self.assertEqual(len(county_ag_results), 5)
        result = county_ag_results[0]
        self.assertEqual(result.jurisdiction, "Adair")
        self.assertEqual(result.votes, 2298)
        self.assertEqual(result.vote_breakdowns['absentee'],
            524)
        self.assertEqual(result.vote_breakdowns['provisional'], 0)

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
        self.assertEqual(len(senate_results), 36)
        result = senate_results[0]
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


class TestExcelPrecinctPost2010ResultLoader(LoaderPrepMixin, TestCase):
    def setUp(self):
        self.loader = ExcelPrecinctPost2010ResultLoader()

    def test_results(self):
        filename = '20100608__ia__primary__adair__precinct.xls'
        mapping = self._get_mapping(filename)
        self._prep_loader_attrs(mapping)

        us_rep_dist_5_rep_results = [r for r in self.loader_results
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
