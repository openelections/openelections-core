import datetime
from mongoengine import *
from mongoengine.fields import (
    BooleanField,
    DateTimeField,
    DictField,
    EmbeddedDocumentField,
    IntField,
    ListField,
    StringField,
)
# Below import is necessary to instantiate connection object
import settings


class Office(EmbeddedDocument):
    state = StringField()
    name = StringField()
    district = StringField()


class Contest(DynamicDocument):
    created = DateTimeField()
    updated = DateTimeField()
    source = StringField(required=True, help_text="Name of data source for this file, preferably standardized filename from datasource.py")
    election_id = StringField(required=True, help_text="election id, e.g. md-2012-11-06-general")
    slug = StringField(required=True, help_text="Slugified office name, plus district and party if relevant")
    state = StringField(required=True)
    start_date = DateTimeField(required=True)
    end_date = DateTimeField(required=True)
    election_type = StringField(help_text="general, primary, etc. from OpenElex metadata")
    result_type = StringField(required=True)
    special = BooleanField(default=False)
    raw_office = StringField(required=True)
    raw_district = StringField()
    raw_party = StringField(help_text="This should only be assigned for closed primaries, where voters must be registered in party to vote in the contest")

    # FIELDS FOR TRANSFORMED/LINKED DATA
    office = EmbeddedDocumentField(Office)
    district = StringField()
    party = StringField(help_text="This should only be assigned for closed primaries, where voters must be registered in party to vote in the contest")

    def __unicode__(self):
        return u'%s-%s' % (self.election_id, self.slug)


class Candidate(DynamicDocument):
    """
    State is included because in nearly all cases, a candidate is unique to a state (presidential candidates run in multiple states,
    but hail from a single state). This would help with lookups and prevent duplicates. Identifiers is a DictField because a candidate
    may have 0 or more identifiers, including state-level IDs.

    parties = ['Democratic', 'Republican'] 

    identifiers = {
        'bioguide' : <bioguide_id>,
        'fec' : [<fecid_1>, <fecid_2>, ...],
        'votesmart' : <votesmart_id>,
        ...
    }

    """
    source = StringField(required=True, help_text="Name of data source for this file, preferably standardized filename from datasource.py")
    election_id = StringField(required=True, help_text="election id, e.g. md-2012-11-06-general")
    contest = ReferenceField(Contest, reverse_delete_rule=CASCADE, required=True)
    contest_slug = StringField(required=True, help_text="Denormalized contest slug for easier querying and obj repr")
    # Keep state denormalized to ease querying?
    state = StringField(max_length=2, required=True, help_text="Capitalized postal code")
    #TODO: Add validation to require raw_full_name or raw_family_name
    raw_full_name = StringField(max_length=300)
    slug = StringField(max_length=300, required=True, help_text="Slugified name for easier querying and obj repr")
    raw_given_name = StringField(max_length=200)
    raw_family_name = StringField(max_length=200)
    raw_suffix = StringField(max_length=200)
    raw_parties = ListField(StringField(), default=list)

    # FIELDS FOR TRANSFORMED/CLEANED DATA or LINKS TO OTHER DATA SETS
    #name = StringField(max_length=300, required=True)
    given_name = StringField(max_length=200)
    family_name = StringField(max_length=200)
    suffix = StringField(max_length=200)
    additional_name = StringField(max_length=200)
    other_names = ListField(StringField(), default=list)
    parties = ListField(StringField(), default=list) # normalized? abbreviations?
    identifiers = DictField()

    def __unicode__(self):
        name =  u'%s - %s' % (self.contest_slug, self.name)
        parties = ""
        if self.raw_parties:
            parties = ", ".join([party for party in self.raw_parties])
            if parties:
                name += " (%s)" % paries
        return name

    @property
    def name(self):
        if self.raw_full_name:
            name =  self.raw_full_name
        else:
            name = "%s, %s" % (self.raw_family_name, self.raw_given_name)
            if self.raw_suffix:
                name += " %s" % self.raw_suff
        return name

class Result(DynamicDocument):

    REPORTING_LEVEL_CHOICES = (
        'state',
        'congressional_district',
        'state_legislative',
        'county',
        'precinct',
        'parish',
    )
    source = StringField(required=True, help_text="Name of data source for this file, preferably standardized filename from datasource.py")
    election_id = StringField(required=True, help_text="election id, e.g. md-2012-11-06-general")
    state = StringField(max_length=2, required=True, help_text="Capitalized postal code")
    reporting_level = StringField(required=True, choices=REPORTING_LEVEL_CHOICES)
    contest = ReferenceField(Contest, reverse_delete_rule=CASCADE, required=True)
    contest_slug = StringField(required=True, help_text="Denormalized contest slug for easier querying and obj repr")
    candidate = ReferenceField(Candidate, reverse_delete_rule=CASCADE, required=True)
    candidate_slug = StringField(required=True, help_text="Denormalized candidate slug for easier querying and obj repr")
    ocd_id = StringField()
    #TODO: Add validation to require raw_jurisdiction or jurisdiction
    raw_jurisdiction = StringField(help_text="Political geography from raw results, if present. E.g. county name, congressional district, precinct number.")
    raw_total_votes = IntField(required=True)
    raw_winner = StringField()
    raw_write_in = StringField()
    raw_vote_breakdowns = DictField(help_text="If provided, store vote totals for election day, absentee, provisional, etc.")

    jurisdiction = StringField(help_text="Derived/standardized political geography, typically when not found in raw results.")
    total_votes = IntField()
    winner = BooleanField(help_text="Winner as determined by OpenElex, if not provided natively in data")
    write_in = BooleanField()
    #vote_breakdowns = DictField(help_text="If provided, store vote totals for election day, absentee, provisional, etc.")

    def __unicode__(self):
        bits = (
            self.contest_slug,
            self.candidate_slug,
            self.reporting_level,
            self.raw_jurisdiction,
            self.raw_total_votes,
        )
        return u'%s-%s-%s-%s (%s)' % bits
