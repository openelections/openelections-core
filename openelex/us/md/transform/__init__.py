from datetime import datetime

from mongoengine import Q
from nameparser import HumanName

from openelex.base.transform import registry
from openelex.models import Candidate, Contest, Office, Party, RawResult, Result


PARTY_MAP = {
    'BOT': 'UNF', 
}
"""
Map of party abbreviations as they appear in MD raw results to canonical
abbreviations.
"""

# Lists of fields on RawResult that are contributed to the canonical
# models.  Maybe this makes more sense to be in the model.

# These get copied onto all the related models
meta_fields = ['source', 'election_id', 'state',]
contest_fields = meta_fields + ['start_date', 'end_date',
    'election_type', 'primary_type', 'result_type', 'special',]
candidate_fields = meta_fields + ['full_name',]
result_fields = meta_fields + ['reporting_level', 'jurisdiction',
    'votes', 'total_votes', 'vote_breakdowns']

district_offices = [
    "U.S. House of Representatives",
    "State Senate",
    "House of Delegates",
]

# Caches to avoid hitting the database
office_cache = {}
party_cache = {}

def _get_fields(raw_result, field_names):
    """
    Extract the fields from a RawResult that will be used to
    construct a related model.

    Returns a dict of fields and values that can be passed to the
    model constructor.
    """
    return { k:getattr(raw_result, k) for k in field_names } 


def _clean_office(office):
    if "president" in office.lower():
        return "President" 
    elif "u.s. senat" in office.lower():
        return "U.S. Senate"
    elif "congress" in office.lower():
        return "U.S. House of Representatives"

    return office

def _clean_party(party):
    try:
        return PARTY_MAP[party]
    except KeyError:
        return party

def _strip_leading_zeros(val):
    return val.strip("0")

def _get_office(raw_result):
    office_query = {
        'state': 'MD',
        'name': _clean_office(raw_result.office),
    }

    # Handle president, where state = "US" 
    if office_query['name'] == "President":
        office_query['state'] = "US"

    if office_query['name'] in district_offices:
        office_query['district'] = _strip_leading_zeros(raw_result.district)

    key = Office.make_key(**office_query)
    try:
        return office_cache[key]
    except KeyError:
        office = Office.objects.get(**office_query)
        # TODO: Remove this once I'm sure this always works. It should.
        assert key == office.key
        office_cache[key] = office
        return office

def _get_party(raw_result, attr='party'):
    party = getattr(raw_result, attr)
    if not party:
        return None

    clean_abbrev = _clean_party(party)
    try:
        return party_cache[clean_abbrev]
    except KeyError:
        party = Party.objects.get(abbrev=clean_abbrev)
        party_cache[clean_abbrev] = party
        return party

# TODO: What should we do with existing records?

def get_raw_results_after_2002():
    # Filter raw results for everything newer than 2002
    return RawResult.objects.filter(state='MD', end_date__gte=datetime(2003, 1, 1))

def get_contest_fields(raw_result):
    # Resolve Office and Party related objects
    fields = _get_fields(raw_result, contest_fields)
    if not fields['primary_type']:
        del fields['primary_type']
    fields['office'] = _get_office(raw_result)
    fields['primary_party'] = _get_party(raw_result, 'primary_party')
    return fields

def contest_key(raw_result):
    # HACK: Work around districts put in presidential races in a few cases
    slug = raw_result.contest_slug
    if (_clean_office(raw_result.office) not in district_offices and
        raw_result.district):  
        slug = slug.replace('-' + raw_result.district.lower(), '')
    return (raw_result.election_id, slug)

def create_unique_contests_after_2002():
    contests = []
    seen = set()

    for rr in get_raw_results_after_2002():
        key = contest_key(rr)
        if key not in seen:
            fields = get_contest_fields(rr)
            fields['updated'] = fields['created'] = datetime.now()
            contest = Contest(**fields)
            contests.append(contest)
            seen.add(key)

    Contest.objects.insert(contests, load_bulk=False)

    print "Created %d contests." % len(contests) 

def cached_get_contest(raw_result, cache):
    key = "%s-%s" % (raw_result.election_id, raw_result.contest_slug)
    try:
        return cache[key]
    except KeyError:
        fields = get_contest_fields(raw_result)
        try:
            contest = Contest.objects.get(**fields)
        except Contest.MultipleObjectsReturned:
            print fields
            raise
        cache[key] = contest
        return contest

def get_candidate_fields(raw_result):
    fields = _get_fields(raw_result, candidate_fields)
    name = HumanName(raw_result.full_name)
    fields['given_name'] = name.first
    fields['family_name'] = name.last
    fields['additional_name'] = name.middle
    fields['suffix'] = name.suffix
    return fields

def create_unique_candidates_after_2002():
    contest_cache = {}
    candidates = []
    seen = set()

    for rr in get_raw_results_after_2002():
        # TODO: Is this the right way to handle "Other Write-Ins"
        if "other" in rr.full_name.lower():
            continue
      
        key = (rr.election_id, rr.candidate_slug)
        if key not in seen:
            fields = get_candidate_fields(rr)
            fields['contest'] = cached_get_contest(rr, contest_cache) 
            candidate = Candidate(**fields)
            candidates.append(candidate)
            seen.add(key)

    Candidate.objects.insert(candidates, load_bulk=False)

    print "Created %d candidates." % len(candidates) 

def _parse_winner(raw_result):
    if raw_result.winner == 'Y':
        return True
    else:
        return False

def _parse_write_in(raw_result):
    if raw_result.write_in == 'Y':
        return True
    else:
        return False

def _get_ocd_id(raw_result):
    clean_jurisdiction = _strip_leading_zeros(raw_result.jurisdiction)
    if raw_result.reporting_level == "county":
        return "ocd-division/country:us/state:md/county:%s" % clean_jurisdiction, 
    elif raw_result.reporting_level == "state_legislative":
        return "ocd-division/country:us/state:md/sldl:%s" % clean_jurisdiction
    elif raw_result.reporting_level == "precinct": 
        return "%s/precinct:%s" % (raw_result.county_ocd_id, clean_jurisdiction)
    else: 
        return None

def cached_get_candidate(raw_result, cache):
    key = "%s-%s" % (raw_result.election_id, raw_result.candidate_slug)
    try:
        return cache[key]
    except KeyError:
        fields = get_candidate_fields(raw_result)
        try:
            candidate = Candidate.objects.get(**fields)
        except Candidate.DoesNotExist:
            print fields 
            raise
        cache[key] = candidate 
        return candidate

def create_unique_results_after_2002():
    candidate_cache = {}
    results = []

    for rr in get_raw_results_after_2002():
        fields = _get_fields(rr, result_fields)
        fields['candidate'] = cached_get_candidate(rr, candidate_cache)
        fields['contest'] = fields['candidate'].contest 
        fields['raw_result'] = rr
        fields['party'] = _get_party(rr)
        fields['winner'] = _parse_winner(rr)
        fields['write_in'] = _parse_write_in(rr)
        fields['jurisdiction'] = _strip_leading_zeros(rr.jurisdiction)
        fields['ocd_id'] = _get_ocd_id(rr)
        result = Result(**fields)
        results.append(result)

    Result.objects.insert(results, load_bulk=False)

    print "Created %d results." % len(results)
        

# TODO: When should we create a Person

#def standardize_office_and_district():
#    pass

#def clean_vote_counts():
    #pass

registry.register('md', create_unique_contests_after_2002)
registry.register('md', create_unique_candidates_after_2002)
registry.register('md', create_unique_results_after_2002)
#registry.register('md', standardize_office_and_district)
#registry.register('md', clean_vote_counts)
