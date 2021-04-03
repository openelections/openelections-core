from builtins import object
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

    def json(self):
        return json.loads(md_data)


class TestApi(TestCase):

    @patch('openelex.api.elections.get')
    def test_find(self, mock_get):
        "openelex.api.find method checks response status and returns array of elections"
        mock_get.return_value = FakeApiResponse(200)
        elecs = api.elections.find('md', None)
        self.assertEquals(len(elecs), 15)
