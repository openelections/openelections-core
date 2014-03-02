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
result_fields = meta_fields + ['reporting_level',]

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

def _clean_district(district):
    # Strip leading zeros
    clean = district.strip("0")
    return clean

def _get_office(raw_result):
    office_query = {
        'state': 'MD',
        'name': _clean_office(raw_result.office),
    }

    # Handle president, where state = "US" 
    if office_query['name'] == "President":
        office_query['state'] = "US"

    # TODO: Figure out how to filter offices by district. It looks like
    # the district fields in RawResult are reflecting reporting level
    # rather than the candidate's district.
    if office_query['name'] in ('U.S. House of Representatives',):
        office_query['district'] = _clean_district(raw_result.district)

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

def get_or_create_contest(raw_result):
    # Resolve Office and Party related objects
    fields = _get_fields(raw_result, contest_fields)
    if not fields['primary_type']:
        del fields['primary_type']
    fields['office'] = _get_office(raw_result)
    fields['primary_party'] = _get_party(raw_result, 'primary_party')

    # Create Contest
    return Contest.objects.get_or_create(**fields)

def create_unique_contests_after_2002():
    num_created = 0

    #import ipdb;ipdb.set_trace()
    for rr in get_raw_results_after_2002():
        (contest, created) = get_or_create_contest(rr)
        if created:
            num_created += 1

    print "Created %d contests." % num_created

def get_or_create_candidate(raw_result):
    (contest, created) = get_or_create_contest(raw_result)
    assert not created
    fields = _get_fields(raw_result, candidate_fields)
    fields['contest'] = contest
    name = HumanName(raw_result.full_name)
    fields['given_name'] = name.first
    fields['family_name'] = name.last
    fields['additional_name'] = name.middle
    fields['suffix'] = name.suffix
    return Candidate.objects.get_or_create(**fields)

def create_unique_candidates_after_2002():
    for rr in get_raw_results_after_2002():
        # TODO: Is this the right way to handle "Other Write-Ins"
        if "other" in rr.full_name.lower():
            continue

        (candidate, created) = get_or_create_candidate(rr)
   
def create_unique_results_after_2002(self):
    for rr in get_raw_results_after_2002():
        fields = _get_fields(rr, result_fields)
        fields['candidate'] = get_or_create_candidate(rr)
        fields['contest'] = fields['candidate'].contest 
        fields['raw_result'] = rr
        fields['party'] = _get_party(rr)
        # TODO: Make sure other fields end up in result. Will
        # probably also have to add them to rawresult
        # BOOKMARK
        Result.objects.get_or_create(**fields)

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
