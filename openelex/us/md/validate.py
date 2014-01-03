from openelex.models import Contest, Candidate, Result

def validate_unique_contests():
    """Count of contests should match unique set of election ids"""
    elec_ids_count = len(Contest.objects.filter(state='MD').distinct('election_id'))
    contest_count = Contest.objects.filter(state='MD').count()
    try:
        assert elec_ids_count == contest_count
    except AssertionError:
        raise AssertionError("MD - mismatch between contest count (%s) and election id count (%s)" % (contest_count, elec_ids_count))

def validate_unique_candidates():
    #for each election date
    #count of unique set of candidates should match Candidate.objects.count()
    pass

def validate_name_parsing():
    #Check assortment of names
    #Check that Other was skipped
    pass
