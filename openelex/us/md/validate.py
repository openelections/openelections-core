from openelex.models import Contest, Candidate, Result

#TODO: Add generic test for unique candidacies per contest
#TODO: Add Result validations

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

def validate_result_count_2012_general_state_legislative():
    """Should be 5504 results for the 2012 general election at the state legislative district level""" 
    filter_kwargs = {
        'state': 'MD',
        'election_id': 'md-2012-11-06-general',
        'reporting_level': 'state_legislative',
    }
    assert Result.objects.filter(**filter_kwargs).count() == 5504

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


#def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
#    pass
