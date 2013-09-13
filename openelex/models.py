from mongoengine import Document, EmbeddedDocument
from mongoengine.fields import DateTimeField, DictField, StringField, ListField, BooleanField, EmbeddedDocumentField, IntField

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

class Office(EmbeddedDocument):
    state = StringField()
    name = StringField()
    district = StringField()

class Result(EmbeddedDocument):

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
    reporting_level = StringField(required=True)
    candidate = EmbeddedDocumentField(Candidate)
    office = EmbeddedDocumentField(Office)
    party = StringField()
    votes = IntField()
    winner = BooleanField()

class Contest(Document):
    """
    Do we need other fields (state, year) on this for easy lookups?
    
    other_vote_counts would include provisional, absentee, same-day, overvotes, etc. all are optional.
    """
    election_id = StringField(required=True) # OpenElections generated slug
    total_votes = IntField()
    computed_total_votes = IntField()
    other_vote_counts = DictField()
    results = ListField(EmbeddedDocumentField(Result))
    created = DateTimeField()
    updated = DateTimeField()