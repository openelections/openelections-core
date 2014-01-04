from openelex.models import Contest, Candidate, Result

#TODO: Genericize these to check unique contests/candidates for all elections
#TODO: Add Result validations

def validate_unique_prez_2012_general():
    """Should only be a single contest for 2012 prez general"""
    count = Contest.objects.filter(election_id='md-2012-11-06-general', slug='president-vice-pres').count()
    expected = 1
    try:
        assert count == expected
        print "PASS: %s general prez contest found for 2012" % count
    except AssertionError:
        raise AssertionError("ERROR: expected 2012 general prez contest count (%s) did not match actual count (%s)" % (expected, count))

def validate_obama_candidacies_2012():
    kwargs = {
        'election_id__startswith': 'md-2012',
        'slug': 'barack-obama',
    }
    count = Candidate.objects.filter(**kwargs).count()
    expected = 2
    try:
        assert count == expected
        print "PASS: %s obama candidacies found for %s general" % (count, '2012')
    except AssertionError:
        raise AssertionError("ERROR: expected obama 2012 candidacies (%s) did not match actual count(%s)" % (expected, count))

def validate_obama_primary_candidacy_2012():
    elec_id= 'md-2012-04-03-primary'
    kwargs = {
        'election_id': elec_id,
        'contest_slug': 'president-vice-pres-dem',
        'slug': 'barack-obama',
    }
    try:
        cand = Candidate.objects.get(**kwargs)
        print "PASS: 1 obama primary candidacy found for 2012: %s" % "-".join(cand.key)
    except AssertionError:
        raise AssertionError("ERROR: expected obama primary candidacies (%s) did not match actual count (%s)" % (expected, count))

#def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
#    pass
