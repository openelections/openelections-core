import re
import os

import unicodecsv

from openelex.models import Contest, Candidate, Result
import openelex.us.md.jurisdiction as jurisdiction

#TODO: Add generic test for unique candidacies per contest
#TODO: Add Result validations

# Classes that describe election attributes

class MDElectionDescription(object):
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


class Primary2000ElectionDescription(MDElectionDescription):
    election_id = 'md-2000-03-07-primary'

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
    def num_pres_candidates(self):
        return self.candidate_counts['president-d'] + self.candidate_counts['president-r']

    @property
    def num_senate_candidates(self):
        return self.candidate_counts['us-senate-d'] + self.candidate_counts['us-senate-r']

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(jurisdiction.counties)
        num_results += self.num_pres_candidates * num_counties
        num_results += self.num_senate_candidates * num_counties
        for district in jurisdiction.congressional_districts:
            for party in ('d', 'r'):
                contest = 'us-house-of-representatives-%s-%s' % (district,
                    party)
                num_candidates = self.candidate_counts[contest]
                num_results += len(jurisdiction.congressional_district_to_county_pre_2002[district]) * num_candidates
               

        return num_results

    @property
    def num_congressional_district_results(self):
        num_results = 0
        num_congressional_districts = len(jurisdiction.congressional_districts)
        num_results += self.num_pres_candidates * num_congressional_districts
        for district in jurisdiction.congressional_districts:
            for party in ('d', 'r'):
                contest = 'us-house-of-representatives-%s-%s' % (district,
                    party)
                num_candidates = self.candidate_counts[contest]
                num_results += num_candidates
        return num_results


class General2000ElectionDescription(MDElectionDescription):
    election_id = 'md-2000-11-07-general'

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

    @property
    def num_state_legislative_results(self):
        total_candidates = reduce(lambda x, y: x+y,
            self.candidate_counts.values())
        return total_candidates * len(jurisdiction.state_legislative_districts)

    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(jurisdiction.counties)

        num_results += self.candidate_counts['president'] * num_counties 
        num_results += self.candidate_counts['us-senate'] * num_counties

        for district in jurisdiction.congressional_districts:
            contest = 'us-house-of-representatives-%s' % (district)
            num_candidates = self.candidate_counts[contest]
            num_results += len(jurisdiction.congressional_district_to_county_pre_2002[district]) * num_candidates
           
        return num_results

class Primary2002ElectionDescription(MDElectionDescription):
    election_id = 'md-2002-09-10-primary'

    reporting_levels = ['county']

    def __init__(self):
        self.load_candidate_counts()

    @property
    def num_county_results_congress(self):
        num_results = 0

        for district in jurisdiction.congressional_districts:
            for party in ('d', 'r'):
                contest = 'us-house-of-representatives-%s-%s' % (district,
                    party)
                try:
                    num_candidates = self.candidate_counts[contest]
                    num_results += len(jurisdiction.congressional_district_to_county[district]) * num_candidates
                except KeyError:
                    print district, party
                    pass

        return num_results

    @property
    def num_county_results_state_senate(self):
        num_results = 0

        for district in jurisdiction.state_senate_districts:
            for party in ('d', 'r'):
                contest = 'state-senate-%s-%s' % (district,
                    party)
                try:
                    num_candidates = self.candidate_counts[contest]
                    num_results += len(jurisdiction.state_senate_district_to_county[district]) * num_candidates
                except KeyError:
                    pass
        
        return num_results

    @property
    def num_county_results_state_legislature(self):
        num_results = 0

        for district in jurisdiction.state_legislative_districts:
            for party in ('d', 'r'):
                contest = 'house-of-delegates-%s-%s' % (district.lower(),
                    party)
                try:
                    num_candidates = self.candidate_counts[contest]
                    num_results += len(jurisdiction.state_legislative_district_to_county[district]) * num_candidates
                except KeyError:
                    pass

        return num_results


    @property
    def num_county_results(self):
        num_results = 0
        num_counties = len(jurisdiction.counties)
        num_gov_candidates = (self.candidate_counts['governor-d'] +
            self.candidate_counts['governor-r'])
        num_comptroller_candidates = (self.candidate_counts['comptroller-d'] +
            self.candidate_counts['comptroller-r'])
        num_ag_candidates = (self.candidate_counts['attorney-general-d'] +
            self.candidate_counts['attorney-general-r'])

        num_results += num_gov_candidates * num_counties
        num_results += num_comptroller_candidates * num_counties
        num_results += num_ag_candidates * num_counties
        num_results += self.num_county_results_congress
        num_results += self.num_county_results_state_senate
        num_results += self.num_county_results_state_legislature

        return num_results

# Generic validation helpers

def _validate_contests(election_description):
    expected_contest_slugs = election_description.contests
    contests = Contest.objects.filter(election_id=election_description.election_id)
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

def _validate_candidate_count(election_description):
    candidate_counts = election_description.candidate_counts
    candidates = Candidate.objects.filter(election_id=election_description.election_id)
    for contest_slug, expected_count in candidate_counts.items():
        count = candidates.filter(contest_slug=contest_slug).count() 
        assert count == expected_count, ("There should be %d candidates "
            "for the contest '%s', but there are %d" %
            (expected_count, contest_slug, count))

def _validate_result_count(election_description, reporting_levels=None):
    failed_levels = []
    if reporting_levels == None:
        reporting_levels = election_description.reporting_levels
    for level in reporting_levels:
        try:
            _validate_result_count_for_reporting_level(election_description, level)
        except AssertionError as e:
            print e
            failed_levels.append(level)
    
    assert len(failed_levels) == 0, ("Result count does not match the expected "
        "value for these levels: {0}".format(", ".join(failed_levels)))

def _validate_result_count_for_reporting_level(election_description, level):
    results = Result.objects.filter(election_id=election_description.election_id,
        reporting_level=level)
    expected = getattr(election_description, 'num_%s_results' %
        level)
    count = results.count()
    assert count == expected, ("Expected %d results for reporting level %s. "
        "Instead there are %d" % (expected, level, count))


# Election-specific Validators

def validate_contests_2000_primary():
    """Check that there are the correct number of Contest records for the 2000 primary"""
    _validate_contests(Primary2000ElectionDescription())

def validate_contests_2000_general():
    """Check that there are the correct number of Contest records for the 2000 general election"""
    _validate_contests(General2000ElectionDescription())

def validate_contests_2002_primary():
    """Check that there are the correct number of Contest records for the 2002 primary"""
    _validate_contests(Primary2002ElectionDescription())

def validate_candidate_count_2000_primary():
    """Check that there are the correct number of Candidate records for the 2000 primary"""
    _validate_candidate_count(Primary2000ElectionDescription())

def validate_candidate_count_2000_general():
    """Check that there are the correct number of Candidate records for the 2000 general election"""
    _validate_candidate_count(General2000ElectionDescription())

def validate_candidate_count_2002_primary():
    """Check that there are the correct number of Candidate records for the 2002 primary"""
    _validate_candidate_count(Primary2002ElectionDescription())

def validate_result_count_2000_primary():
    """Should have results for every candidate and contest in 2000 primary"""
    _validate_result_count(Primary2000ElectionDescription())

def validate_2000_primary_congress_county_results():
    """Confirm that county level results are created for congressional races in the 2000 primary"""
    results = Result.objects.filter(state='MD',
        reporting_level='county',
        election_id='md-2000-03-07-primary',
        contest_slug__startswith='us-house-of-representatives')

    district_1_results_d = results.filter(contest_slug='us-house-of-representatives-1-d')
    # 11 counties intersect with district 1 * 4 Democratic candidates = 44
    # results
    count = district_1_results_d.count()
    assert count  == 44, ("There should be 44 results for District 1, instead"
        "there are %d" % count) 
    # Bennett Bozman got 3429 votes in Worcester county
    result = results.get(candidate_slug='bennett-bozman',
        jurisdiction='Worcester')
    assert result.votes == 3429, ("Bennett Bozman should have 3429 votes in "
        "Worcester County, instead has %d" % result.votes) 

    district_8_results_r = results.filter(contest_slug='us-house-of-representatives-8-r')
    # 1 county intersects with district 8 * 1 Republican candidate = 1
    count = district_8_results_r.count()
    assert count == 1, ("There should be 1 result for District 8, instead "
        "there are %d" % count)
    # Constance A. Morella got 35472 votes in Montgomery county 
    result = results.get(candidate_slug='constance-a-morella',
        jurisdiction='Montgomery')
    assert result.votes == 35472, ("Constance A. Morella should have 35472 "
        "votes in Montgomery county.  Instead has %d" % result.votes)

def validate_result_count_2000_general():
    """Should have results for every candidate and contest in 2000 general election"""
    # TODO: Include precincts if it's not too hard
    reporting_levels = ['county', 'state_legislative']
    _validate_result_count(General2000ElectionDescription(),
        reporting_levels)

def validate_result_count_2002_primary():
    """Should have results for every candidate and contest in 2002 primary"""
    _validate_result_count(Primary2002ElectionDescription())

def validate_result_count_2012_general_state_legislative():
    """Should be 5504 results for the 2012 general election at the state legislative district level""" 
    filter_kwargs = {
        'state': 'MD',
        'election_id': 'md-2012-11-06-general',
        'reporting_level': 'state_legislative',
    }
    # TODO: Do this in election description based way
    assert Result.objects.filter(**filter_kwargs).count() == 5504

def validate_unique_prez_2012_general():
    """Should only be a single contest for 2012 prez general"""
    count = Contest.objects.filter(election_id='md-2012-11-06-general', slug='president').count()
    expected = 1
    try:
        assert count == expected
        print "PASS: %s general prez contest found for 2012" % count
    except AssertionError:
        raise AssertionError("expected 2012 general prez contest count (%s) did not match actual count (%s)" % (expected, count))

def validate_obama_candidacies_2012():
    """Should only be two Obama candidacies in 2012 (primary and general)"""
    kwargs = {
        'election_id__startswith': 'md-2012',
        'slug': 'barack-obama',
    }
    count = Candidate.objects.filter(**kwargs).count()
    expected = 2
    try:
        assert count == expected
        print "PASS: %s obama candidacies found for %s" % (count, '2012')
    except AssertionError:
        raise AssertionError("expected obama 2012 candidacies (%s) did not match actual count(%s)" % (expected, count))

def validate_obama_primary_candidacy_2012():
    """Should only be one Obama primary candidacy for 2012"""
    elec_id= 'md-2012-04-03-primary'
    kwargs = {
        'election_id': elec_id,
        'contest_slug': 'president-d',
        'slug': 'barack-obama',
    }
    try:
        cand = Candidate.objects.get(**kwargs)
        print "PASS: 1 obama primary candidacy found for 2012: %s" % "-".join(cand.key)
    except Candidate.DoesNotExist:
        raise Candidate.DoesNotExist("zero obama primary candidacies found for 2012")
    except Candidate.MultipleObjectsReturned as e:
        raise Candidate.MultipleObjectsReturned("multiple obama primary candidacies found for 2012: %s" %  e)


# Validators that are not specific to a particular election

def validate_aggregate_congressional_district_results():
    """Validate that results have been correctly aggregated from congressional districts split by county"""

    election_id = 'md-2000-03-07-primary'

    # President
    results = Result.objects.filter(election_id=election_id,
        contest_slug='president-d', reporting_level='congressional_district')
    # Maryland has 8 congressional districts
    count = len(results.distinct('jurisdiction'))
    assert count == 8, ("There should be results for 8 congressional "
        "districts.  Instead there are results for %d." % count)
    # 4 candidates * 8 districts = 32 results
    count = results.count()
    assert count == 32, ("There should be 32 results.  Instead there are %d" %
        count)
    # Al Gore got 32426 votes in district 1
    votes = results.get(candidate_slug='al-gore', jurisdiction='1').votes
    assert votes == 32426, ("Al Gore should have 32426 votes in District 1. "
        "Instead there are %d" % votes)

    # U.S. House 
    results = Result.objects.filter(election_id=election_id,
        contest_slug='us-house-of-representatives-1-r',
        reporting_level='congressional_district')
    # Only 1 candidate in 1 district
    count = results.count()
    assert count == 1, ("There should be 1 result.  Instead there are %d" %
        count)
    # Wayne T. Gilchrest got 49232 votes
    votes = results.get(candidate_slug='wayne-t-gilchrest', jurisdiction='1').votes
    assert votes == 49232, ("Wayne T. Gilchrest should have 49232 votes in "
        "District 1. Instead he has %d." % votes) 

    election_id = 'md-2008-02-12-primary'
    
    # President
    results = Result.objects.filter(election_id=election_id,
        contest_slug='president-d', reporting_level='congressional_district')
    # Maryland has 8 congressional districts
    count = len(results.distinct('jurisdiction'))
    assert count == 8, ("There should be results for 8 congressional "
        "districts.  Instead there are results for %d." % count)
    # 9 candidates * 8 districts = 72 results
    count = results.count()
    assert count == 72, ("There should be 72 results.  Instead there are %d" %
        count)
    votes = results.get(candidate_slug='hillary-clinton', jurisdiction='6').votes 
    assert votes == 34322, ("Hillary Clinton should have 34322 votes in "
        "District 6, instead she has %d" % votes) 

    election_id = 'md-2012-04-03-primary'

    # President
    results = Result.objects.filter(election_id=election_id,
        contest_slug='president-d', reporting_level='congressional_district')
    # Maryland has 8 congressional districts
    count = len(results.distinct('jurisdiction'))
    assert count == 8, ("There should be results for 8 congressional "
        "districts.  Instead there are results for %d." % count)
    # 2 candidates * 8 disctricts = 16 results
    count = results.count()
    assert count == 16, ("There should be 16 results.  Instead there are %d" %
        count)
    votes = results.get(candidate_slug='barack-obama', jurisdiction='1').votes
    assert votes == 20343, ("Barack Obama should have 20343 votes in District "
        "1, instead he has %d" % votes)

def validate_precinct_names_normalized():
    """Precinct jurisdiction names should be in the format 'NN-NNN'"""
    precincts = Result.objects.filter(state='MD', reporting_level='precinct').distinct('jurisdiction')
    for precinct in precincts:
        assert re.match(r'\d+-0\d\d', precinct), ("Precinct %s doesn't match "
            "normalized format." % precinct)

def validate_unique_contests():
    """Should have a unique set of contests for all elections"""
    # Get all election ids
    election_ids = list(Contest.objects.distinct('election_id'))
    for elec_id in election_ids:
        contests = Contest.objects.filter(election_id=elec_id)
        # compare the number of contest records to unique set of contests for that election
        count = contests.count()
        expected = len(list(contests.distinct('slug')))
        try:
            assert expected == count
        except AssertionError:
            raise AssertionError("%s contests expected for elec_id '%s', but %s found" % (expected, elec_id, count))
    print "PASS: unique contests counts found for all elections"

#def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
#    pass
