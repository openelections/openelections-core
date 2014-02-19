import os
from datetime import datetime
from unittest import TestCase

#from mongoengine.context_managers import query_counter

from openelex.exceptions import UnsupportedFormatError
from openelex.tests.mongo_test_case import MongoTestCase
from openelex.tests.factories import (ContestFactory, CandidateFactory,
    ResultFactory)

from openelex.models import Contest, Candidate
from openelex.base.bake import Roller, Baker


class TestRoller(MongoTestCase):
    def setUp(self):
        # Call super to select the test database
        super(TestRoller, self).setUp()

        # Create some test models 
        contest = ContestFactory()
        candidate = CandidateFactory(contest=contest)
        result = ResultFactory(candidate=candidate, contest=contest)

    def test_get_list(self):
        roller = Roller()
        data = roller.get_list(state='md', datefilter='20120403',
                type='general', level='state_legislative')
        from pprint import pprint
        pprint(data[0])
        self.fail()

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

    def test_collect_items(self):
        baker = Baker(state='md', datefilter='20120403',
                type='general', level='state_legislative')
        data = baker.collect_items().get_items()
        print data[0]
            
        self.fail()
