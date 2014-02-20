from datetime import date

import factory
from factory.mongoengine import MongoEngineFactory
from factory.fuzzy import FuzzyChoice, FuzzyInteger

from openelex.lib.text import slugify
from openelex.models import Candidate, Contest, Result


class ContestFactory(MongoEngineFactory):
    FACTORY_FOR = Contest

    state = "MD"
    start_date = date.today()
    updated = date.today()
    election_type = "general"
    result_type = "certified"
    raw_office = "President - Vice Pres"

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

    @factory.lazy_attribute
    def slug(self):
        return slugify(self.raw_office, "-") 


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
    reporting_level = FuzzyChoice(Result.REPORTING_LEVEL_CHOICES)
    candidate = factory.SubFactory(CandidateFactory)
    contest = factory.SubFactory(ContestFactory)
    raw_total_votes = FuzzyInteger(2000) 
    ocd_id = "ocd-division/country:us/state:md"
    # TODO: This just randomly assigns a value and doesn't reflect
    # the real world where there's only one winner per election
    winner = FuzzyChoice([True, False])
    write_in = False

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

    @factory.lazy_attribute
    def total_votes(self):
        return self.raw_total_votes
