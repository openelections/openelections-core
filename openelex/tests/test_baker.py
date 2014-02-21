import os
from datetime import datetime
from unittest import TestCase

#from mongoengine.context_managers import query_counter
from mongoengine import Q

from openelex.exceptions import UnsupportedFormatError
from openelex.tests.mongo_test_case import MongoTestCase
from openelex.tests.factories import (ContestFactory, CandidateFactory,
    ResultFactory)

from openelex.models import Contest, Candidate, Result
from openelex.base.bake import Roller, Baker


class TestRoller(MongoTestCase):
    """
    Data-bound tests for the Roller class.
    """
    
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
        'name_raw',
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

    def _create_models(self, start_date, state="MD", election_type="general"):
        """Create some election models for a given date"""
        contest = ContestFactory(start_date=start_date,
            election_type=election_type)
        candidate = CandidateFactory(contest=contest)
        result = ResultFactory(candidate=candidate, contest=contest)

    def setUp(self):
        # Call super to select the test database
        super(TestRoller, self).setUp()

        # Create some test models 
        self._create_models(datetime(2012, 11, 6), election_type="general")
        self._create_models(datetime(2012, 4, 3), election_type="primary")

        self.roller = Roller()

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


class TestBaker(TestCase):
    def test_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        fmt = 'json'
        filename = baker.filename(fmt=fmt, timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615.json')

    def test_manifest_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        filename = baker.manifest_filename(timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615_manifest.txt')

    def test_write_unsupported_format(self):
        baker = Baker(state='md')
        self.assertRaises(UnsupportedFormatError, baker.write, 'xml')

    def test_default_outputdir(self):
        baker = Baker(state='md')
        path = os.path.join(os.path.join('openelex', 'us', 'bakery'))
        outputdir = baker.default_outputdir()
        self.assertTrue(outputdir.endswith(path))
