import logging
import re

from nameparser import HumanName

from openelex.base.transform import Transform, registry
from openelex.models import Candidate, Contest, Office, Party, RawResult, Result
from openelex.lib.text import ocd_type_id
from openelex.lib.insertbuffer import BulkInsertBuffer

# Instantiate logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


meta_fields = ['source', 'election_id', 'state',]

contest_fields = meta_fields + ['start_date',
                                'end_date',
                                'election_type',
                                'primary_type',
                                'result_type',
                                'special',
                                ]
candidate_fields = meta_fields + ['full_name', 'given_name',
                                  'family_name', 'additional_name']
result_fields = meta_fields + ['reporting_level', 'jurisdiction',
                               'votes', 'total_votes', 'vote_breakdowns']

STATE = 'WA'


class BaseTransform(Transform):

    """
    Base class that encapsulates shared functionality for other Washinton
    transforms.
    """

    PARTY_MAP = {
        # Unaffiliated
        'Nonpartisan': 'UN',
        '(States No Party Preference)': 'UN',
        '(Prefers Non Partisan Party)': 'UN',
        '(Prefers Neither Party)': 'UN',
        '(Prefers Non-partisan Party)': 'UN',

        # Independent
        'Independent Candidates': 'I',
        '(Prefers Independent Party)': 'I',
        '(Prefers ReganIndependent Party)': 'I',
        '(Prefers Independent - No Party)': 'I',
        '(Prefers Independent Dem. Party)': 'I',
        '(Prefers Centrist Party)': 'I',
        '(Prefers Independent No Party)': 'I',
        '(Prefers Independent Dem Party)': 'I',
        '(Prefers Independent-Gop Party)': 'I',
        '(Prefers Prog Independent Party)': 'I',
        '(Prefers Indep Republican Party)': 'I',


        # Republican
        'Republican': 'R',
        'Republican Party Nominees': 'R',
        '(Prefers Republican Party)': 'R',
        '(Prefers G.O.P. Party)': 'R',
        '(Prefers (G.O.P.) Party)': 'R',
        '(Prefers G O P Party)': 'R',
        '(Prefers R Party)': 'R',
        '(Prefers Cut Taxes G.O.P. Party)': 'R',
        '(Prefers Grand Old Party)': 'R',
        '(Prefers (R) Problemfixer Party)': 'R',
        '(Prefers GOP Party)': 'R',
        '(Prefers Conservative Party)': 'R',
        '(Prefers GOP  Party)': 'R',
        '(Prefers Tea Party)': 'R',
        '(Prefers Gop Party)': 'R',
        '(Prefers (R) Hope&change Party)': 'R',
        '(Prefers    Republican Party)': 'R',

        # Democrat
        'Democrat': 'D',
        'Democratic Party Nominees': 'D',
        '(Prefers Democratic Party)': 'D',
        '(Prefers Progressive Dem. Party)': 'D',
        '(Prefers Progressive Party)': 'D',
        '(Prefers  True Democratic Party)': 'D',
        '(Prefers Progressive Dem Party)': 'D',
        '(Prefers Demo Party)': 'D',
        '(Prefers Prolife Democrat Party)': 'D',
        '(Prefers F.D.R. Democrat Party)': 'D',
        '(Prefers Democracy Indep. Party)': 'D',
        '(Prefers Democratic-Repub Party)': 'D',

        # Libertarian
        '(Prefers Libertarian Party)': 'LIB',
        'Libertarian Party Nominees': 'LIB',

        # Green
        '(Prefers Green Party)': 'GRE',
        'Green Party Nominees': 'GRE',

        # Constitution
        '(Prefers Constitution Party)': 'CON',  # What's abbr for this?
        'Constitution Party Nominees': 'CON',

        # Party of Commons
        '(Prefers Party Of Commons Party)': 'COM',  # What's abbr for this?

        # Socialist
        # Not sure which this is
        'Socialism & Libertarian Party Nominees': 'SOC',
        'Socialist Workers Party Nominees': 'SOC',
        '(Prefers Socialist Altern Party)': 'SOC',

        # Etc
        '(Prefers Reform Party': 'UK',
        '(Prefers America\'s Third Party)': 'UK',
        '(Prefers Salmon Yoga Party)': 'UK',
        '(Prefers Lower Taxes Party)': 'UK',
        '(Prefers Bull Moose Party)': 'UK',
        '(Prefers Happiness Party)': 'UK',
        '(Prefers  SeniorSide Party)': 'UK',
        'Justice Party Nominees': 'UK',
        '(Prefers The 99%% Party)': 'UK',
        '(Prefers Employmentwealth Party)': 'UK',
        '(Prefers The Human Rights Party)': 'UK',
        '(Prefers Neopopulist Party)': 'UK'

    }

    target_offices = set([
        'President',
        'U.S. Senator',
        'U.S. Representative',
        'Governor',
        'Secretary of State',
        'Superintendent of Public Instruction',
        'State Senator',
        'State Representative',
        'Lt. Governor',
        'Governor',
        'Treasurer',
        'Auditor',
        'State Superintendent of Public Instruction',
        'Attorney General',
        'Commissioner of Public Lands'
    ])

    def __init__(self):
        super(BaseTransform, self).__init__()
        self._office_cache = {}
        self._party_cache = {}
        self._contest_cache = {}

    def get_raw_results(self):
        return RawResult.objects.filter(state=STATE).no_cache()

    def get_contest_fields(self, raw_result):
        fields = self._get_fields(raw_result, contest_fields)
        if not fields['primary_type']:
            del fields['primary_type']
        fields['office'] = self._get_office(raw_result)
        fields['primary_party'] = self.get_party(raw_result, 'primary')

        return fields

    def _get_fields(self, raw_result, field_names):
        #print {k: getattr(raw_result, k) for k in field_names}
        #quit('FIELD NAMES')
        return {k: getattr(raw_result, k) for k in field_names}


    def _get_office(self, raw_result):
        print {k:getattr(raw_result, k) for k in raw_result}
        #quit('doskosk')
        office_query = {
            'state': STATE,
            'name': self._clean_office(raw_result.office)
        }

        if office_query['name'] is 'President':
            office_query['state'] = 'US'

        if office_query['name'] in self.target_offices:
            office_query['district'] = raw_result.district

        key = Office.make_key(**office_query)
        print Office
        print key + "\n\n"

        try:
            return self._office_cache[key]
        except KeyError:
            try:
                office = Office.objects.get(**office_query)
                assert key == office.key
                self._office_cache[key] = office
                return office
            except Office.DoesNotExist:
                logger.error("No office matching query {}".format(office_query))
                raise


    def _strip_leading_zeros(self, val):
        print val
        quit('hi')
        return val.lstrip("0")

    def get_party(self, raw_result, attr='party'):
        party = getattr(raw_result, attr)
        if not party:
            return None

        clean_abbrev = self._clean_party(party)
        if not clean_abbrev:
            return None

        try:
            return self._party_cache[clean_abbrev]
        except KeyError:
            try:
                party = Party.objects.get(abbrev=clean_abbrev)
                self._party_cache[clean_abbrev] = party
                return party
            except Party.DoesNotExist:
                logger.error("No party with abbreviation {}".format(clean_abbrev))
                raise

    def _clean_party(self, party):
        try:
            return self.PARTY_MAP[party]
        except KeyError:
            return None

    def _clean_office(self, office):
        """
        See: https://github.com/openelections/core/blob/dev/openelex/us/wa/load.py#L370

        """

        presidential_regex = re.compile('president', re.IGNORECASE)
        senate_regex = re.compile('(senate|senator)', re.IGNORECASE)
        house_regex = re.compile('(house|representative)', re.IGNORECASE)
        governor_regex = re.compile('governor', re.IGNORECASE)
        treasurer_regex = re.compile('treasurer', re.IGNORECASE)
        auditor_regex = re.compile('auditor', re.IGNORECASE)
        sos_regex = re.compile('secretary', re.IGNORECASE)
        lt_gov_regex = re.compile(r'(lt|Lt|Lieutenant)', re.IGNORECASE)
        ospi_regex = re.compile(
            'superintendent of public instruction',
            re.IGNORECASE)
        ag_regex = re.compile('attorney general', re.IGNORECASE)
        wcpl_regex = re.compile('commissioner of public lands', re.IGNORECASE)
        local_regex = re.compile(
            r'(\bState\b|Washington|Washington\s+State|Local|'
            'Legislative District)',
            re.IGNORECASE)
        national_regex = re.compile(
            r'(U\.S\.|^US$|Congressional|National|United\s+States|U\.\s+S\.\s+)',
            re.IGNORECASE)

        if re.search(house_regex, office):
            if re.search(national_regex, office):
                return 'U.S. Representative'
            elif re.search(local_regex, office):
                return 'State Representative'
            else:
                return None
        elif re.search(governor_regex, office):
            return 'Governor'
        elif re.search(wcpl_regex, office):
            return 'Commissioner of Public Lands'
        elif re.search(senate_regex, office):
            if re.search(national_regex, office):
                return 'U.S. Senator'
            elif re.search(local_regex, office):
                return 'State Senator'
            else:
                return None
        elif re.search(lt_gov_regex, office):
            return 'Lt. Governor'
        elif re.search(ospi_regex, office):
            return 'Superintendent of Public Instruction'
        elif re.search(sos_regex, office):
            return 'Secretary of State'
        elif re.search(treasurer_regex, office):
            return 'Treasurer'
        elif re.search(auditor_regex, office):
            return 'Auditor'
        elif re.search(ag_regex, office):
            return 'Attorney General'
        elif re.search(presidential_regex, office):
            return 'President'
        else:
            return None

    def get_candidate_fields(self, raw_result):
        year = raw_result.end_date.year

        fields = self._get_fields(raw_result, candidate_fields)

        name = HumanName(raw_result.full_name)
        fields['given_name'] = name.first
        fields['family_name'] = name.last
        try:
            fields['additional_name'] = name.middle
            fields['suffix'] = name.suffix
        except Exception,e:
            logger.error(e)
        return fields


    def get_contest(self, raw_result):
        """
        Returns the Contest model instance for a given RawResult.

        Caches the result in memory to reduce the number of calls to the
        datastore.
        """
        key = "%s-%s" % (raw_result.election_id, raw_result.contest_slug)
        try:
            return self._contest_cache[key]
        except KeyError:
            fields = self.get_contest_fields(raw_result)
            fields.pop('source')
            try:
                contest = Contest.objects.get(**fields)
            except Exception:
                print fields
                raise
            self._contest_cache[key] = contest
            return contest

class CreateContestsTransform(BaseTransform):
    name = 'create_unique_contests'

    def __call__(self):
        contests = []
        seen = set()

        for result in self.get_raw_results():
            key = self._contest_key(result)
            if key not in seen:
                fields = self.get_contest_fields(result)
                fields['updated'] = fields['created'] = datetime.now()
                contest = Contest(**fielnds)
                contests.append(contest)
                seen.add(key)


        Contest.objects.insert(contests, load_bulk=False)
        logger.info("Created {} contests.".format(len(contests)))


    def reverse(self):
        old = Contest.objects.filter(state=STATE)
        logger.info('\tDeleting {} previously created contests'.format(old.count()))
        old.delete()

    def _contest_key(self, raw_result):
        slug = raw_result.contest_slug
        return (raw_result.election_id, slug)

registry.register('wa', CreateContestsTransform)