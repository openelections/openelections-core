import os

import unicodecsv

from openelex.models import Contest, Candidate, Result
from openelex.us.md import jurisdiction

# Classes that describe election attributes

class MDElection(object):
    """
    Base class for describing Maryland elections.

    Subclasses, should, at the very least, define ``election_id`` and
    ``candidate_counts`` attributes.

    It will also likely be useful to define ``num_{{reporting_level}}_results``
    attributes that contain the known number of results for a particular
    reporting level.
    """

    election_id = None
    """
    Identifier for election.

    This should match the ID from the OpenElections metadata API.
    """

    candidate_counts = {}
    """
    Map of contest slugs to known number of candidates.
    """

    reporting_levels = []
    """
    Iterable of available reporting levels of results in this election.
    """

    counties = jurisdiction.counties
    congressional_districts = jurisdiction.congressional_districts
    state_senate_districts = jurisdiction.state_senate_districts
    state_legislative_districts = jurisdiction.state_legislative_districts
    state_senate_district_to_county = jurisdiction.state_senate_district_to_county
    state_legislative_district_to_county = jurisdiction.state_legislative_district_to_county
   
    @property
    def contests(self):
        """
        Return a list of contest slugs.
        """
        return self.candidate_counts.keys()

    def candidate_counts_filename(self): 
        bits = self.election_id.split('-')
        tpl ="candidate_counts__{year}{month}{day}__{state}__{election_type}.csv"
        return tpl.format(year=bits[1], month=bits[2], day=bits[3],
            state=bits[0], election_type=bits[4])

    def load_candidate_counts(self, skip_zero=True):
        """
        Load candidate counts from a CSV fixture
        
        Args: 

        skip_zero: Should contests with zero candidates be ignored? Default is True.
        """
        pwd = os.path.abspath(os.path.dirname(__file__)) 
        filename = os.path.join(pwd, 'fixtures', self.candidate_counts_filename())
        with open(filename, 'rU') as csvfile:
            self.candidate_counts = {}
            reader = unicodecsv.DictReader(csvfile)
            for row in reader:
                count = int(row['count'].strip())
                contest = row['contest'].strip()
                if count == 0 and skip_zero:
                    continue
                self.candidate_counts[contest] = count

    def get_party_contests(self, contest):
        contests = []
        if self.race_type == 'primary' and self.primary_type == 'closed':
            for party in ('d', 'r'):
                contests.append('{0}-{1}'.format(contest, party))
        else:
            contests.append(contest)

        return contests

    def _get_candidate_count(self, base_contest):
        count = 0
        for contest in self.get_party_contests(base_contest):
            try:
                count += self.candidate_counts[contest]
            except KeyError:
                print "WARN: no candidate count for contest '{0}'".format(
                    contest)

        return count
        
    def _get_num_district_results(self, contest_slug, districts,
            district_to_county=None):
        num_results = 0
        contest_tpl = contest_slug + '-{0}'

        for district in districts:
            base_contest = contest_tpl.format(district.lower())
            for contest in self.get_party_contests(base_contest):
                try:
                    num_candidates = self.candidate_counts[contest]
                    if district_to_county:
                        num_results += len(district_to_county[district]) * num_candidates
                    else:
                        num_results += num_candidates
                except KeyError:
                    pass

        return num_results

    # Generic validation helpers

    def validate_contests(self):
        expected_contest_slugs = self.contests
        contests = Contest.objects.filter(election_id=self.election_id)
        expected = len(expected_contest_slugs)
        count = contests.count()
        assert count == expected, ("There should be %d contests, but there are %d" %
            (expected, count))

        for slug in expected_contest_slugs: 
            try:
                contests.get(slug=slug)
            except Contest.DoesNotExist:
                raise Contest.DoesNotExist("No contest with slug '%s' found" %
                    slug)

    def validate_candidate_count(self):
        candidate_counts = self.candidate_counts
        candidates = Candidate.objects.filter(election_id=self.election_id)
        for contest_slug, expected_count in candidate_counts.items():
            count = candidates.filter(contest_slug=contest_slug).count() 
            assert count == expected_count, ("There should be %d candidates "
                "for the contest '%s', but there are %d" %
                (expected_count, contest_slug, count))

    def validate_result_count(self, reporting_levels=None):
        failed_levels = []
        if reporting_levels == None:
            reporting_levels = self.reporting_levels
        for level in reporting_levels:
            try:
                self._validate_result_count_for_reporting_level(level)
            except AssertionError as e:
                print e
                failed_levels.append(level)
        
        assert len(failed_levels) == 0, ("Result count does not match the expected "
            "value for these levels: {0}".format(", ".join(failed_levels)))

    def _validate_result_count_for_reporting_level(self, level):
        results = Result.objects.filter(election_id=self.election_id,
            reporting_level=level)
        expected = getattr(self, 'num_%s_results' %
            level)
        count = results.count()
        assert count == expected, ("Expected %d results for reporting level %s. "
            "Instead there are %d" % (expected, level, count))


class CountyCongressResultsMixin(object):
    @property
    def num_county_results_congress(self):
        return self._get_num_district_results('us-house-of-representatives',
            self.congressional_districts,
            self.congressional_district_to_county)


class CountyStateSenateResultsMixin(object):
    @property
    def num_county_results_state_senate(self):
        return self._get_num_district_results('state-senate',
            self.state_senate_districts,
            self.state_senate_district_to_county)


class CountyStateLegislatureResultsMixin(object):
    @property
    def num_county_results_state_legislature(self):
        return self._get_num_district_results('house-of-delegates',
            self.state_legislative_districts,
            self.state_legislative_district_to_county)


class StateLegislativeResultsMixin(object):
    @property
    def num_state_legislative_results(self):
        total_candidates = reduce(lambda x, y: x+y,
            self.candidate_counts.values())
        return total_candidates * len(self.state_legislative_districts)


class Election2000(CountyCongressResultsMixin, MDElection):
    congressional_district_to_county = jurisdiction.congressional_district_to_county_pre_2002
   
    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_pres_candidates = self._get_candidate_count('president')
        num_senate_candidates = self._get_candidate_count('us-senate')

        num_results += num_pres_candidates * num_counties
        num_results += num_senate_candidates * num_counties
        num_results += self.num_county_results_congress

        return num_results


class Election2000Primary(Election2000):
    election_id = 'md-2000-03-07-primary'

    race_type = 'primary'

    primary_type = 'closed'

    reporting_levels = ['county', 'congressional_district']

    candidate_counts = {
        # 4 candidates, including "Uncommitted To Any Presidential Candidate"
        'president-d': 4,
        'president-r': 6,
        'us-senate-d': 3,
        'us-senate-r': 8,
        'us-house-of-representatives-1-d': 4,
        'us-house-of-representatives-1-r': 1,
        'us-house-of-representatives-2-d': 4,
        'us-house-of-representatives-2-r': 1,
        'us-house-of-representatives-3-d': 1,
        'us-house-of-representatives-3-r': 1,
        'us-house-of-representatives-4-d': 2,
        'us-house-of-representatives-4-r': 1,
        'us-house-of-representatives-5-d': 2,
        'us-house-of-representatives-5-r': 1,
        'us-house-of-representatives-6-d': 4,
        'us-house-of-representatives-6-r': 2,
        'us-house-of-representatives-7-d': 1,
        'us-house-of-representatives-7-r': 2,
        'us-house-of-representatives-8-d': 5,
        'us-house-of-representatives-8-r': 1,
    }

    @property
    def num_congressional_district_results(self):
        num_results = 0
        num_congressional_districts = len(self.congressional_districts)
        num_pres_candidates = self._get_candidate_count('president')

        num_results += num_pres_candidates * num_congressional_districts
        num_results += self._get_num_district_results(
            'us-house-of-representatives',
            self.congressional_districts)

        return num_results


class Election2000General(StateLegislativeResultsMixin, Election2000):
    election_id = 'md-2000-11-07-general'

    race_type = 'general'

    candidate_counts = {
        'president': 21,
        'us-senate': 4,
        'us-house-of-representatives-1': 4,
        'us-house-of-representatives-2': 3,
        'us-house-of-representatives-3': 4,
        'us-house-of-representatives-4': 4,
        'us-house-of-representatives-5': 3,
        'us-house-of-representatives-6': 4,
        'us-house-of-representatives-7': 3,
        'us-house-of-representatives-8': 6,
    }


class Election2002(CountyCongressResultsMixin, CountyStateSenateResultsMixin,
        CountyStateLegislatureResultsMixin, MDElection):
    reporting_level = ['county']

    congressional_district_to_county = jurisdiction.congressional_district_to_county_2002

    def __init__(self):
        self.load_candidate_counts()

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_gov_candidates = self._get_candidate_count('governor')
        num_comptroller_candidates = self._get_candidate_count('comptroller')
        num_ag_candidates = self._get_candidate_count('attorney-general')

        num_results += num_gov_candidates * num_counties
        num_results += num_comptroller_candidates * num_counties
        num_results += num_ag_candidates * num_counties
        num_results += self.num_county_results_congress
        num_results += self.num_county_results_state_senate
        num_results += self.num_county_results_state_legislature

        return num_results


class Election2002Primary(Election2002):
    election_id = 'md-2002-09-10-primary'

    race_type = 'primary'

    primary_type = 'closed'


class Election2002General(Election2002):
    election_id = 'md-2002-11-05-general'

    race_type = 'general'

    def __init__(self):
        self.load_candidate_counts()


class Election2004(StateLegislativeResultsMixin, CountyCongressResultsMixin,
        MDElection):
    reporting_levels = ['county', 'precinct', 'state_legislative']

    congressional_district_to_county = jurisdiction.congressional_district_to_county_2002

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_pres_candidates = self._get_candidate_count('president')
        num_senate_candidates = self._get_candidate_count('us-senate')

        num_results += num_pres_candidates * num_counties
        num_results += num_senate_candidates * num_counties
        num_results += self.num_county_results_congress

        return num_results


class Election2004Primary(Election2004):
    election_id = 'md-2004-03-02-primary'

    race_type = 'primary'

    primary_type = 'closed'

    candidate_counts = {
        'president-d': 12,
        'president-r': 1,
        'us-senate-d': 3,
        'us-senate-r': 9,
        'us-house-of-representatives-1-d': 4,
        'us-house-of-representatives-1-r': 2,
        'us-house-of-representatives-2-d': 1,
        'us-house-of-representatives-2-r': 3,
        'us-house-of-representatives-3-d': 2,
        'us-house-of-representatives-3-r': 3,
        'us-house-of-representatives-4-d': 2,
        'us-house-of-representatives-4-r': 6,
        'us-house-of-representatives-5-d': 1,
        'us-house-of-representatives-5-r': 3,
        'us-house-of-representatives-6-d': 7,
        'us-house-of-representatives-6-r': 2,
        'us-house-of-representatives-7-d': 2,
        'us-house-of-representatives-7-r': 3,
        'us-house-of-representatives-8-d': 3,
        'us-house-of-representatives-8-r': 3,
    }


class Election2004General(Election2004):
    election_id = 'md-2004-11-02-general'

    race_type = 'general'

    candidate_counts = {
        'president': 12,
        'us-senate': 8,
        'us-house-of-representatives-1': 3,
        'us-house-of-representatives-2': 4,
        'us-house-of-representatives-3': 4,
        'us-house-of-representatives-4': 5,
        'us-house-of-representatives-5': 5,
        'us-house-of-representatives-6': 4,
        'us-house-of-representatives-7': 4,
        'us-house-of-representatives-8': 4,
    }


class Election2006(StateLegislativeResultsMixin, CountyCongressResultsMixin,
        CountyStateSenateResultsMixin, CountyStateLegislatureResultsMixin, 
        MDElection):
    reporting_levels = ['county', 'precinct', 'state_legislative']

    congressional_district_to_county = jurisdiction.congressional_district_to_county_2002

    def __init__(self):
        self.load_candidate_counts()

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_gov_candidates = self._get_candidate_count('governor')
        num_comptroller_candidates = self._get_candidate_count('comptroller')
        num_ag_candidates = self._get_candidate_count('attorney-general')
        num_senate_candidates = self._get_candidate_count('us-senate')

        num_results += num_gov_candidates * num_counties
        num_results += num_comptroller_candidates * num_counties
        num_results += num_ag_candidates * num_counties
        num_results += num_senate_candidates * num_counties

        num_results += self.num_county_results_congress
        num_results += self.num_county_results_state_senate
        num_results += self.num_county_results_state_legislature

        return num_results


class Election2006Primary(Election2006):
    election_id = 'md-2006-09-12-primary'
    race_type = 'primary'
    primary_type = 'closed'


class Election2006General(Election2006):
    election_id = 'md-2006-11-07-general'
    race_type = 'general'


class Election2008(CountyCongressResultsMixin, StateLegislativeResultsMixin, MDElection):
    reporting_levels = ['county', 'precinct', 'state_legislative']

    congressional_district_to_county = jurisdiction.congressional_district_to_county_2002

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_pres_candidates = self._get_candidate_count('president')

        num_results += num_pres_candidates * num_counties
        num_results += self.num_county_results_congress

        return num_results


class Election2008Primary(Election2008):
    election_id = 'md-2008-02-12-primary'
    race_type = 'primary'
    primary_type = 'closed'
    candidate_counts = {
      'president-d': 9,
      'president-r': 9,
      'us-house-of-representatives-1-d': 4,
      'us-house-of-representatives-1-r': 5,
      'us-house-of-representatives-2-d': 1,
      'us-house-of-representatives-2-r': 1,
      'us-house-of-representatives-3-d': 2,
      'us-house-of-representatives-3-r': 4,
      'us-house-of-representatives-4-d': 6,
      'us-house-of-representatives-4-r': 4,
      'us-house-of-representatives-5-d': 2,
      'us-house-of-representatives-5-r': 3,
      'us-house-of-representatives-6-d': 5,
      'us-house-of-representatives-6-r': 5,
      'us-house-of-representatives-7-d': 2,
      'us-house-of-representatives-7-r': 2,
      'us-house-of-representatives-8-d': 3,
      'us-house-of-representatives-8-r': 5,
    }


class Election2008General(Election2008):
    election_id = 'md-2008-06-17-general'
    race_type = 'general'
    candidate_counts = {
      'president': 24,
      'us-house-of-representatives-1': 4,
      'us-house-of-representatives-2': 4,
      'us-house-of-representatives-3': 3,
      'us-house-of-representatives-4': 7,
      'us-house-of-representatives-5': 4,
      'us-house-of-representatives-6': 4,
      'us-house-of-representatives-7': 6,
      'us-house-of-representatives-8': 7,
    }

class Election2010(StateLegislativeResultsMixin, CountyCongressResultsMixin,
        CountyStateSenateResultsMixin, CountyStateLegislatureResultsMixin, 
        MDElection):
    reporting_levels = ['county', 'precinct', 'state_legislative']

    congressional_district_to_county = jurisdiction.congressional_district_to_county_2002

    def __init__(self):
        self.load_candidate_counts()

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_gov_candidates = self._get_candidate_count('governor')
        num_comptroller_candidates = self._get_candidate_count('comptroller')
        num_ag_candidates = self._get_candidate_count('attorney-general')
        num_senate_candidates = self._get_candidate_count('us-senate')

        num_results += num_gov_candidates * num_counties
        num_results += num_comptroller_candidates * num_counties
        num_results += num_ag_candidates * num_counties
        num_results += num_senate_candidates * num_counties

        num_results += self.num_county_results_congress
        num_results += self.num_county_results_state_senate
        num_results += self.num_county_results_state_legislature

        return num_results

class Election2010Primary(Election2010):
    election_id = 'md-2010-09-14-primary'
    race_type = 'primary'
    primary_type = 'closed'


class Election2010General(Election2010):
    election_id = 'md-2010-11-02-general'
    race_type = 'general'


class Election2012(StateLegislativeResultsMixin, CountyCongressResultsMixin,
        MDElection):
    reporting_levels = ['county', 'precinct', 'state_legislative']

    congressional_district_to_county = jurisdiction.congressional_districts_to_county_2011

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(self.counties)
        num_pres_candidates = self._get_candidate_count('president')
        num_senate_candidates = self._get_candidate_count('us-senate')

        num_results += num_pres_candidates * num_counties
        num_results += num_senate_candidates * num_counties
        num_results += self.num_county_results_congress

        return num_results


class Election2012Primary(Election2012):
    election_id = 'md-2012-04-03-primary'
    race_type = 'primary'
    primary_type = 'closed'

    candidate_counts = {
      'president-d': 2,
      'president-r': 8,
      'us-senate-d': 9,
      'us-senate-r': 10,
      'us-house-of-representatives-1-d': 3,
      'us-house-of-representatives-1-r': 1,
      'us-house-of-representatives-2-d': 1,
      'us-house-of-representatives-2-r': 6,
      'us-house-of-representatives-3-d': 2,
      'us-house-of-representatives-3-r': 4,
      'us-house-of-representatives-4-d': 3,
      'us-house-of-representatives-4-r': 4,
      'us-house-of-representatives-5-d': 2,
      'us-house-of-representatives-5-r': 3,
      'us-house-of-representatives-6-d': 5,
      'us-house-of-representatives-6-r': 8,
      'us-house-of-representatives-7-d': 3,
      'us-house-of-representatives-7-r': 2,
      'us-house-of-representatives-8-d': 2,
      'us-house-of-representatives-8-r': 4,
    }


class Election2012General(Election2012):
    election_id = 'md-2012-11-06-general'
    race_type = 'general'

    candidate_counts = {
      'president': 37,
      'us-senate': 9,
      'us-house-of-representatives-1': 7,
      'us-house-of-representatives-2': 5,
      'us-house-of-representatives-3': 4,
      'us-house-of-representatives-4': 4,
      'us-house-of-representatives-5': 5,
      'us-house-of-representatives-6': 4,
      'us-house-of-representatives-7': 6,
      'us-house-of-representatives-8': 5,
    }

