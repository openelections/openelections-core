import json
from collections import OrderedDict
from mock import MagicMock
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

# load fixture globally to avoid reloads
with open('fixtures/election_api_response_md.json','r') as f:
    md_data = json.load(f)


class TestApi(TestCase):

    def setUp(self):
        api.get = MagicMock(return_value=md_data)

    def test_find_all_elections(self):
        "Api find returns all elections with just state"
        elecs = api.elections.find('md')
        self.assertEquals(len(elecs), 15)

    #def test_all_races_in_one_year(self):
        #"Api supports search for elections for state"
        #self.api.search('md', datefilter='2012')
