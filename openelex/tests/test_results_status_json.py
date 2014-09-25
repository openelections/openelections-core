from unittest import TestCase

from openelex.tasks.bake import reporting_level_status

class TestResultsStatusJSON(TestCase):
    """
    Test the results_status_json task and supporting functions
    """
    def test_reporting_level_status(self):
        election1 = {
            'absentee_and_provisional': False,
            'cong_dist_level': False,
            'cong_dist_level_status': "",
            'county_level': True,
            'county_level_status': "",
            'direct_link': "http://elections.utah.gov/Media/Default/2012%20Canvass/2012%20General%20Canvass%20Report.xls",
            'direct_links': [
                "http://elections.utah.gov/Media/Default/2012%20Canvass/2012%20General%20Canvass%20Report.xls"
            ],
            'end_date': "2012-11-06",
            'gov': True,
            'house': True,
            'id': 824,
            'organization': {
                'city': "Salt Lake City",
                'fec_page': "",
                'gov_agency': True,
                'gov_level': "state",
                'id': 45,
                'name': "Utah Lieutenant Governor",
                'resource_uri': "/api/v1/organization/45/",
                'slug': "utah-lieutenant-governor",
                'state': "UT",
                'street': "Utah State Capitol, Suite 220",
                'url': "http://elections.utah.gov/election-resources/election-results"
            },
            'portal_link': "http://elections.utah.gov/election-resources/election-results",
            'precinct_level': False,
            'precinct_level_status': "",
            'prez': True,
            'primary_note': "",
            'primary_type': "",
            'race_type': "general",
            'resource_uri': "/api/v1/election/824/",
            'result_type': "certified",
            'senate': True,
            'special': False,
            'start_date': "2012-11-06",
            'state': {
                'name': "Utah",
                'postal': "UT",
                'resource_uri': "/api/v1/state/UT/"
            },
            'state_leg': True,
            'state_leg_level': False,
            'state_leg_level_status': "",
            'state_level': True,
            'state_level_status': "",
            'state_officers': True,
            'user_fullname': "Willis, Derek"
        }

        election2 ={
            u'absentee_and_provisional': True,
            u'cong_dist_level': False,
            u'cong_dist_level_status': u'',
            u'county_level': True,
            u'county_level_status': u'baked-raw',
            u'direct_link': u'http://www.elections.state.md.us/elections/2012/election_data/index.html',
            u'direct_links': [u'http://www.elections.state.md.us/elections/2012/election_data/index.html'],
            u'end_date': u'2012-11-06',
            u'gov': False,
            u'house': True,
            u'id': 259,
            u'organization': {u'city': u'Annapolis',
                u'fec_page': u'http://www.fec.gov/pubrec/cfsdd/mddir.htm',
                u'gov_agency': True,
                u'gov_level': u'state',
                u'id': 5,
                u'name': u'Maryland State Board of Elections',
                u'resource_uri': u'/api/v1/organization/5/',
                u'slug': u'maryland-state-board-elections',
                u'state': u'MD',
                u'street': u'151 West Street, Suite 200',
                u'url': u'http://www.elections.state.md.us/index.html'},
            u'portal_link': u'http://www.elections.state.md.us/elections/2012/results/general/index.html',
            u'precinct_level': True,
            u'precinct_level_status': u'baked-raw',
            u'prez': True,
            u'primary_note': u'',
            u'primary_type': u'',
            u'race_type': u'general',
            u'resource_uri': u'/api/v1/election/259/',
            u'result_type': u'certified',
            u'senate': True,
            'slug': u'md-2012-11-06-general',
            u'special': False,
            u'start_date': u'2012-11-06',
            u'state': {u'name': u'Maryland',
                u'postal': u'MD',
                u'resource_uri': u'/api/v1/state/MD/'},
            u'state_leg': True,
            u'state_leg_level': True,
            u'state_leg_level_status': u'baked-raw',
            u'state_level': True,
            u'state_level_status': u'',
            u'state_officers': False,
            u'user_fullname': u'Hing, Geoffrey'
        } 

        # When an election doesn't have a status set, use results availability
        status = reporting_level_status(election1, 'county')
        self.assertEqual(status, 'yes')

        # When an election has status set, use the status value
        status = reporting_level_status(election2, 'county')
        self.assertEqual(status, 'baked-raw')
        status = reporting_level_status(election2, 'cong_dist')
        self.assertEqual(status, '')
