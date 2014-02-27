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

office_cache = {}
party_cache = {}

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

def _get_party(raw_result):
    clean_abbrev = _clean_party(raw_result.party)
    try:
        return party_cache[clean_abbrev]
    except KeyError:
        party = Party.objects.get(abbrev=clean_abbrev)
        party_cache[clean_abbrev] = party
        return party

def create_unique_contests_after_2002():
    timestamp = datetime.now()
    # Filter raw results for everything newer than 2002
    raw_results = RawResult.objects.filter(state='MD', end_date__gte=datetime(2003, 1, 1))
    contests = []
    #import ipdb;ipdb.set_trace()
    for rr in raw_results:
        # Resolve Office and Party related objects
        office = _get_office(rr)
        party = _get_party(rr)

        kwargs = rr._data.copy()
        kwargs.pop('id')
        kwargs.pop('created')
        kwargs.pop('updated')
        # Create Contest
        #contest = Contest(**kwargs)
        #contests.append(contest)

    #TODO: save Contest instances

#TODO: create_unique_candidates_after_2002
#TODO: create_unique_results_after_2002

def parse_names_after_2002():
    cands = Candidate.objects.filter(
        (Q(election_id__not__contains="2000") | Q(election_id__not__contains="2002")),
        state='MD'
    )
    #Loop through candidates, perform name parse and update
    for cand in cands:
        # Skip Other write-ins
        if 'other' in cand.slug:
            continue
        name = HumanName(cand.raw_full_name)
        cand.given_name = name.first
        cand.family_name = name.last
        cand.additional_name = name.middle
        cand.suffix = name.suffix
        cand.save()

#def standardize_office_and_district():
#    pass

#def clean_vote_counts():
    #pass

registry.register('md', create_unique_contests_after_2002)
registry.register('md', parse_names_after_2002)
#registry.register('md', standardize_office_and_district)
#registry.register('md', clean_vote_counts)
