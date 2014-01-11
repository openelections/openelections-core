from mongoengine import Q
from nameparser import HumanName

from openelex.base.transform import registry
from openelex.models import Candidate

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

registry.register('md', parse_names_after_2002)
#registry.register('md', standardize_office_and_district)
#registry.register('md', clean_vote_counts)
