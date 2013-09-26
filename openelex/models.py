from mongoengine import *
from mongoengine.fields import DateTimeField, DictField, StringField, ListField, BooleanField, EmbeddedDocumentField, IntField
import datetime

class Candidate(EmbeddedDocument):
    """
    State is included because in nearly all cases, a candidate is unique to a state (presidential candidates run in multiple states,
    but hail from a single state). This would help with lookups and prevent duplicates. Identifiers is a DictField because a candidate
    may have 0 or more identifiers, including state-level IDs.
    """
    uuid = StringField()
    state = StringField(required=True)
    given_name = StringField(max_length=200)
    additional_name = StringField(max_length=200)
    family_name = StringField(max_length=200)
    suffix = StringField(max_length=200)
    name = StringField(max_length=300, required=True)
    other_names = ListField(StringField(), default=list)
    raw_parties = ListField(StringField(), default=list)
    parties = ListField(StringField(), default=list) # normalized? abbreviations?
    identifiers = DictField()
    
    """
    parties = ['Democratic', 'Republican'] 
    
    identifiers = {
        'bioguide' : <bioguide_id>,
        'fec' : [<fecid_1>, <fecid_2>, ...],
        'votesmart' : <votesmart_id>,
        ...
    }
    
    """

class Office(Document):
    state = StringField()
    name = StringField()
    district = StringField()

class Result(Document):

    REPORTING_LEVEL_CHOICES = (
        'congressional_district',
        'state_legislative',
        'precinct',
        'parish',
        'precinct',
        'county',
        'state',
    )
    jurisdiction = StringField()
    ocd_id = StringField()
    raw_office = StringField()
    reporting_level = StringField(required=True)
    candidate = EmbeddedDocumentField(Candidate)
    write_in = BooleanField(default=False)
    office = ReferenceField(Office)
    party = StringField()
    total_votes = IntField(default=0)
    vote_breakdowns = DictField() # if vote totals are included for election day, absentee, provisional, etc, put them here.
    winner = BooleanField()

class Contest(Document):
    """
    Do we need other fields (state, year) on this for easy lookups?
    
    other_vote_counts would include provisional, absentee, same-day, overvotes, etc. all are optional.
    """
    state = StringField(required=True)
    year = IntegerField(required=True)
    election_id = StringField(required=True) # OpenElections generated slug
    start_date = DateField()
    end_date = DateField()
    election_type = StringField()
    offices = ListField()
    reporting_levels = ListField()
    absentee_provisional = BooleanField(default=False)
    source_url = StringField()
    result_type = StringField()
    special = BooleanField(default=False)
    results = ListField(ReferenceField(Result))
    created = DateTimeField()
    updated = DateTimeField()