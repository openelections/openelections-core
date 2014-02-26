from datetime import datetime
from pprint import pprint

from mongoengine import Q
from nameparser import HumanName

from openelex.base.transform import registry
from openelex.models import Candidate, Contest, Office, Party, RawResult, Result


def _translate_office_name(office):
    if "president" in office.lower():
        return "President" 
    elif "u.s. senat" in office.lower():
        return "U.S. Senate"
    elif "congress" in office.lower():
        return "U.S. House of Representatives"

    return office

def create_unique_contests_after_2002():
    timestamp = datetime.now()
    # Filter raw results for everything newer than 2002
    raw_results = RawResult.objects.filter(state='MD', end_date__gte=datetime(2003, 1, 1))
    #TODO: create office and party lookups
    #Where are we storing these mappings?
    office_lkup = ""
    party_lkup = ""
    contests = []
    #import ipdb;ipdb.set_trace()
    for rr in raw_results:
        office_query = {
            'state': 'MD',
            'name': _translate_office_name(rr.office),
        }

        # Handle president, where state = "US" 
        if office_query['name'] == "President":
            office_query['state'] = "US"

        # TODO: Figure out how to filter offices by district. It looks like
        # the district fields in RawResult are reflecting reporting level
        # rather than the candidate's district.
        if office_query['name'] in ('U.S. House of Representatives',):
            office_query['district'] = rr.district

        try:
            office = Office.objects.get(**office_query)
        except Exception:
            pprint(rr._data)
            print office_query 
            raise
        kwargs = rr._data.copy()
        kwargs.pop('id')
        kwargs.pop('created')
        kwargs.pop('updated')
        # Resolve Office and Party embedded objects
        # Create Contest
        contest = Contest(**kwargs)
        contests.append(contest)

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
