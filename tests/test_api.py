import json
from os.path import abspath, dirname, join
from collections import OrderedDict
from mock import patch
from unittest import TestCase

from openelex import api
from openelex.api.base import prepare_api_params


class TestUrlBuilder(TestCase):

    def test_default_params(self):
        "default params should include format and limit"
        expected = OrderedDict([('format', 'json'), ('limit', '0')])
        actual = prepare_api_params({})
        self.assertEquals(expected, actual)

    def test_default_params_values(self):
        "params should always be alphabetized, and end with format and limit"
        params = [
            ('start_date=', '2012-11-02'),
            ('end_date=',   '2012-11-02'),
        ]
        unordered = OrderedDict(params)

        ordered = params[:]
        ordered.sort()
        ordered.extend([('format', 'json'), ('limit', '0')])
        expected = OrderedDict(ordered)

        actual = prepare_api_params(unordered)
        self.assertEquals(expected, actual)

tests_dir = abspath(dirname(__file__))
fixture_path = join(tests_dir, 'fixtures/election_api_response_md.json')
with open(fixture_path, 'r') as f:
    md_data = f.read()


class FakeApiResponse(object):

    def __init__(self, status):
        self.status_code = status
        self.content = md_data

class TestApi(TestCase):

    @patch('openelex.api.elections.get')
    def test_find_all_elections(self, mock_get):
        "Api find returns all elections with just state"
        mock_get.return_value = FakeApiResponse(200)
        elecs = api.elections.find('md')['objects']
        self.assertEquals(len(elecs), 15)

    #def test_all_races_in_one_year(self):
        #"Api supports search for elections for state"
        #self.api.search('md', datefilter='2012')
