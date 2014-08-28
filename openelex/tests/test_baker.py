import os
from datetime import date, datetime
from unittest import TestCase

from mongoengine import Q

from openelex.exceptions import UnsupportedFormatError
from openelex.tests.mongo_test_case import MongoTestCase
from openelex.tests.factories import (ContestFactory, CandidateFactory,
    OfficeFactory, RawResultFactory, ResultFactory)

from openelex.models import Candidate, RawResult, Result
from openelex.base.bake import (FlattenFieldTransform, RawResultRoller, ResultRoller,
    Baker, RawBaker,
    reporting_levels_for_election)


class FieldTransformTestCase(TestCase):
    def test_flatten_field_transform(self):
        transform = FlattenFieldTransform(RawResult, 'vote_breakdowns')
        test_data = {
            'vote_breakdowns': {
                'election_night_total': 15,
                'absentee_total': 2,
                'provisional_total': 3,
                'second_absentee_total': 4,
            }
        }
        data = transform.transform(test_data.copy())
        self.assertNotIn('vote_breakdowns', data)
        for k, v in test_data['vote_breakdowns'].items():
            self.assertEqual(data[k], v)

class RollerTestCase(MongoTestCase):
    OUTPUT_FIELDS = [
        # From Election spec
        'id',
        'year',
        'start_date',
        'end_date',
        'division',
        'result_type',
        'election_type',
        'special',
        'updated_at',

        # These are flattened since the rows in the output
        # are result-level
        #'offices',
        #'reporting_levels',

        # These don't have corresponding data in the models.
        # See https://github.com/openelections/core/issues/45 
        #'absentee_provisional',
        #'notes',
        #'source_url',

        # From Result spec
        'first_name',
        'middle_name',
        'last_name',
        'suffix',
        # TODO: Renable this, but it requires wiring RawResult into the ResultRoller
        #'name_raw',
        'party',
        'winner',
        'votes',
        'write_in',

        # These don't have corresponding data in the models.
        # See https://github.com/openelections/core/issues/45 
        #'pct',
        #'precincts',
        
    ]
    """
    Expected output fields, taken from specs at
    https://github.com/openelections/specs/  
    """

class TestResultRoller(RollerTestCase):
    """
    Data-bound tests for the ResultRoller class.
    """

    def _create_models(self, start_date, state="MD", election_type="general"):
        """Create some election models for a given date"""
        office = OfficeFactory()
        contest = ContestFactory(start_date=start_date,
            election_type=election_type, office=office)
        candidate = CandidateFactory(contest=contest)
        ResultFactory(candidate=candidate, contest=contest)

    def setUp(self):
        # Call super to select the test database
        super(TestResultRoller, self).setUp()

        # Create some test models 
        self._create_models(datetime(2012, 11, 6), election_type="general")
        self._create_models(datetime(2012, 4, 3), election_type="primary")

        self.roller = ResultRoller()

    def test_primary_collection_name(self):
        self.assertEqual(self.roller.primary_collection_name, "result")

    def _test_date_filter_matches(self, datestring, election_id="md-2012-11-06-general"):
        """
        Test that the query filter built for a given datestring matches an
        election_id.
        """
        q = self.roller.build_date_filters(datestring)
        self.assertTrue(isinstance(q, Q))
        q_dict = q.to_query(Result)
        self.assertIn('election_id', q_dict)
        # The election_id value in the dict should be a regex that matches an
        # election for that date 
        m = q_dict['election_id'].search(election_id)
        self.assertIsNotNone(m)

    def test_build_date_filters(self):
        # Test that different supported date formats build working filters
        self._test_date_filter_matches("2012")
        self._test_date_filter_matches("201211")
        self._test_date_filter_matches("20121106")

        # Test that a ValueError is raised for an unsupported date format
        self.assertRaises(ValueError, self.roller.build_date_filters, "201200")

    def test_get_list_no_results(self):
        data = self.roller.get_list(state='tx')
        self.assertEqual(len(data), 0)

    def test_get_list_has_fields(self):
        data = self.roller.get_list(state='md', datefilter='20121106',
            type='general')
        row = data[0]
        for field in self.OUTPUT_FIELDS:
            self.assertIn(field, row)

    def test_get_list(self):
        data = self.roller.get_list(state='md', datefilter='20121106',
            type='general')
        self.assertNotEqual(len(data), 0)
        self.assertEqual(len(data),
            Result.objects(election_id__contains='md-2012-11-06-general').count())
        # TODO: Test this further

    def test_get_list_filter_by_level(self):
        level = 'precinct'

        # Make sure there's at least one result at this level
        if Result.objects(reporting_level=level).count() == 0:
            candidate = Candidate.objects()[0]
            contest = candidate.contest
            ResultFactory(candidate=candidate, contest=contest,
                reporting_level=level)

        data = self.roller.get_list(state="MD", reporting_level=level)
        self.assertEqual(len(data),
            Result.objects(state="MD", reporting_level=level).count())

    def test_get_fields_no_data(self):
        """Test the list of output fields when no data has been fetched"""
        fields = set(self.roller.get_fields())
        for field in self.OUTPUT_FIELDS:
            self.assertIn(field, fields)


class TestRawResultRoller(RollerTestCase):
    def setUp(self):
        # Call super to select the test database
        super(TestRawResultRoller, self).setUp()

        state = 'MD'
        start_date = date(2000, 3, 7)
        RawResultFactory(state=state, start_date=start_date)

        self.roller = RawResultRoller()

    def test_get_list_has_fields(self):
        data = self.roller.get_list(state='md', datefilter='20000307')
        row = data[0]
        for field in self.OUTPUT_FIELDS:
            self.assertIn(field, row)

        # vote_breakdowns shouldn't be in the output dict
        self.assertNotIn('vote_breakdowns', row)
        # But the keys in vote_breakdowns should be
        self.assertIn('election_night_total', row)
        self.assertIn('absentee_total', row)
        self.assertIn('provisional_total', row)
        self.assertIn('second_absentee_total', row)

    def test_get_fields_has_fields(self):
        data = self.roller.get_list(state='md', datefilter='20000307')
        fields = self.roller.get_fields()
        # vote_breakdowns shouldn't be in the output fields 
        self.assertNotIn('vote_breakdowns', fields)
        # But the keys in vote_breakdowns should be
        self.assertIn('election_night_total', fields)
        self.assertIn('absentee_total', fields)
        self.assertIn('provisional_total', fields)
        self.assertIn('second_absentee_total', fields)


class TestBaker(TestCase):
    def test_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        fmt = 'json'
        filename = baker.filename(fmt=fmt, timestamp=ts, state='md')
        self.assertEqual(filename, 'md_20140211T105615.json')

    def test_manifest_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        filename = baker.manifest_filename(timestamp=ts, state='md')
        self.assertEqual(filename, 'md_20140211T105615_manifest.txt')

    def test_write_unsupported_format(self):
        baker = Baker(state='md')
        self.assertRaises(UnsupportedFormatError, baker.write, 'xml')

    def test_default_outputdir(self):
        baker = Baker(state='md')
        path = os.path.join(os.path.join('openelex', 'us', 'bakery'))
        outputdir = baker.default_outputdir()
        self.assertTrue(outputdir.endswith(path))


class TestRawBaker(MongoTestCase):
    def test_filename(self):
        state = 'md'
        baker = RawBaker(state=state)
        fmt = 'csv'
        filename = baker.filename(fmt, state=state, datefilter='20000307',
            election_type='primary')
        self.assertEqual(filename, "20000307__md__primary__raw.csv")
        filename = baker.filename(fmt, state=state, datefilter='20000307',
            election_type='primary', reporting_level='precinct')
        self.assertEqual(filename, "20000307__md__primary__precinct__raw.csv")

    def test_collect_items(self):
        state = 'MD'
        start_date = date(2000, 3, 7)
        RawResultFactory(state=state, start_date=start_date)
        RawResultFactory(state=state, start_date=date(2014, 4, 3))
        baker = RawBaker(state=state, datefilter=start_date.strftime("%Y%m%d"))
        items = baker.get_items()
        self.assertEqual(len(items), 0)
        baker.collect_items()
        items = baker.get_items()
        self.assertEqual(len(items),
            RawResult.objects.filter(start_date=start_date).count())
        # TODO: Test dates of filtered items


class TestUtilitiesWithDatabase(MongoTestCase):
    def test_reporting_levels_for_election(self):
        start_date = date(2000, 3, 7)
        state = 'MD'
        expected_levels = ['precinct', 'county']
        for level in expected_levels:
            RawResultFactory(state=state, start_date=start_date,
                reporting_level=level, election_type='general')
        levels = reporting_levels_for_election(state, start_date.strftime("%Y%m%d"),
           'general', raw=True)
        self.assertEqual(len(levels), len(expected_levels))
        for level in expected_levels:
            self.assertIn(level, levels)
