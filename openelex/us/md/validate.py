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

#def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
#    pass
