from datetime import date

import factory
from factory.mongoengine import MongoEngineFactory
from factory.fuzzy import FuzzyChoice, FuzzyInteger

from openelex.lib.text import slugify
from openelex.models import (Candidate, Contest, Office, RawResult, Result,
    REPORTING_LEVEL_CHOICES)

class OfficeFactory(MongoEngineFactory):
    FACTORY_FOR = Office

    state = "MD"
    name = "House of Delegates"
    district = "35B"
    chamber = "lower"


class ContestFactory(MongoEngineFactory):
    FACTORY_FOR = Contest

    state = "MD"
    start_date = date.today()
    updated = date.today()
    election_type = "general"
    result_type = "certified"

    @factory.lazy_attribute
    def election_id(self):
        datestr = self.start_date.strftime("%Y-%m-%d")
        return "%s-%s-%s" % (self.state.lower(), datestr, self.election_type)

    @factory.lazy_attribute
    def source(self):
        # 20121106__md__general__state_legislative.csv  
        datestr = self.start_date.strftime("%Y%m%d")
        return "%s__%s__%s__%s.csv" % (datestr, self.state.lower(),
            self.election_type, "state_legislative")

    @factory.lazy_attribute
    def end_date(self):
        return self.start_date


GIVEN_NAME_CHOICES = [
    'Hiram',
    'Jeannette',
    'Romualdo',
    'Daniel',
]

FAMILY_NAME_CHOICES = [
    'Revels',
    'Rankin',
    'Pacheco',
    'Fong',
    'Inouye',
]


class CandidateFactory(MongoEngineFactory):
    FACTORY_FOR = Candidate

    state = "MD"
    raw_given_name = FuzzyChoice(GIVEN_NAME_CHOICES)
    raw_family_name = FuzzyChoice(FAMILY_NAME_CHOICES)
    contest = factory.SubFactory(ContestFactory)
    additional_name = ""
    raw_suffix = ""

    @factory.lazy_attribute
    def election_id(self):
        return self.contest.election_id

    @factory.lazy_attribute
    def source(self):
        return self.contest.source

    @factory.lazy_attribute
    def raw_full_name(self):
        return "%s %s" % (self.raw_given_name, self.raw_family_name)

    @factory.lazy_attribute
    def contest_slug(self):
        return self.contest.slug

    @factory.lazy_attribute
    def given_name(self):
        return self.raw_given_name

    @factory.lazy_attribute
    def family_name(self):
        return self.raw_family_name

    @factory.lazy_attribute
    def slug(self):
        return slugify(self.raw_full_name, '-')

    @factory.lazy_attribute
    def suffix(self):
        return self.raw_suffix


class ResultFactory(MongoEngineFactory):
    FACTORY_FOR = Result

    state = 'MD'
    reporting_level = FuzzyChoice(REPORTING_LEVEL_CHOICES)
    candidate = factory.SubFactory(CandidateFactory)
    contest = factory.SubFactory(ContestFactory)
    votes = FuzzyInteger(2000) 
    total_votes = FuzzyInteger(2000)
    ocd_id = "ocd-division/country:us/state:md"
    jurisdiction = "Maryland"
    # TODO: This just randomly assigns a value and doesn't reflect
    # the real world where there's only one winner per election
    winner = FuzzyChoice([True, False])
    write_in = False
    party = FuzzyChoice(["DEM", "REP",])

    @factory.lazy_attribute
    def contest_slug(self):
        return self.contest.slug

    @factory.lazy_attribute
    def candidate_slug(self):
        return self.candidate.slug

    @factory.lazy_attribute
    def election_id(self):
        return self.contest.election_id

    @factory.lazy_attribute
    def source(self):
        return self.contest.source

class RawResultFactory(MongoEngineFactory):
    FACTORY_FOR = RawResult

    # Meta fields
    state = 'MD'

    @factory.lazy_attribute
    def source(self):
        date_slug = self.start_date.strftime("%Y%m%d")
        return "{}__{}.csv".format(self.state, date_slug)

    @factory.lazy_attribute
    def election_id(self):
        date_slug = self.start_date.strftime("%Y-%m-%d")
        return "{}-{}-{}".format(self.state.lower(),
            date_slug, self.election_type)

    # Contest fields
    election_type = FuzzyChoice(["primary", "general"])
    result_type = 'certified'
    office = "House of Delegates"

    @factory.lazy_attribute
    def end_date(self):
        return self.start_date

    # Candidate fields
    full_name = "Bill Bradley"
    suffix = ""

    # Result fields
    reporting_level = 'county'
    party = FuzzyChoice(['Democratic', 'Republican'])
    jurisdiction = 'Allegany'
    votes = FuzzyInteger(2000) 
    vote_breakdowns = {
        'election_night_total': 5,
        'absentee_total': 3,
        'provisional_total': 4,
        'second_absentee_total': 2,
    }
    winner = FuzzyChoice(["Winner", ""])
    write_in = "Write-In" 
