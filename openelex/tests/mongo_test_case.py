from unittest import TestCase

from mongoengine import connect, ConnectionError
from nose.exc import SkipTest

from openelex.settings import MONGO


class MongoTestCase(TestCase):

    def setUp(self):
        try:
            self.db = connect('openelex_test', **MONGO['openelex_test'])['openelex_test']
        except ConnectionError:
            raise SkipTest('Could not connect to Mongo on localhost')

    def tearDown(self):
        for collection in self.db.collection_names():
            if collection == 'system.indexes':
                continue
            self.db.drop_collection(collection)
