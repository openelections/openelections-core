from unittest import TestCase

from mongoengine import ConnectionError
from nose.exc import SkipTest

from openelex.settings import init_db


class MongoTestCase(TestCase):

    def setUp(self):
        try:
            self.db = init_db('openelex_test')
        except ConnectionError:
            raise SkipTest('Could not connect to Mongo on localhost')

    def tearDown(self):
        for collection in self.db.collection_names():
            if collection == 'system.indexes':
                continue
            self.db.drop_collection(collection)
