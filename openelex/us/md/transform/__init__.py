from mongoengine import Q
from nameparser import HumanName

from openelex.base.transform import registry
from openelex.models import Candidate

class After2002Files(object):

    @staticmethod
    def parse_names():
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

    @staticmethod
    def standardize_office_and_district():
        pass

    @staticmethod
    def clean_vote_counts():
        pass

registry.register('md', After2002Files.parse_names, 'after2002')
#registry.register('md', After2002Files.standardize_office_and_district, 'after2002')
#registry.register('md', After2002Files.clean_vote_counts, 'after2002')
