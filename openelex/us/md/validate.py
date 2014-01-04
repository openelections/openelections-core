from openelex.models import Contest, Candidate, Result

#TODO: Genericize this to check unique contests for all elections
def validate_unique_prez_2012_general():
    """Should only be a single contest for 2012 prez general"""
    count = Contest.objects.filter(election_id='md-2012-11-06-general', slug='president-vice-pres').count()
    expected = 1
    try:
        assert count == expected
        print "PASS: %s general prez election found for 2012" % count
    except AssertionError:
        raise AssertionError("Mismatch between 2012 general prez contest count (%s) and expected count (%s)" % (count, expected))

#def validate_unique_candidates():
    #for each election date
    #count of unique set of candidates should match Candidate.objects.count()
#    pass

#def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
#    pass
