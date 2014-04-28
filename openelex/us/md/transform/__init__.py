from datetime import datetime
import logging

from nameparser import HumanName

from openelex.base.transform import Transform, registry
from openelex.models import Candidate, Contest, Office, Party, RawResult, Result
from openelex.lib.text import ocd_type_id
from openelex.lib.insertbuffer import BulkInsertBuffer
from ..validate import (validate_precinct_names_normalized,
    validate_no_baltimore_city_comptroller,
    validate_uncommitted_primary_state_legislative_results)


# Lists of fields on RawResult that are contributed to the canonical
# models.  Maybe this makes more sense to be in the model.

# Meta fields get copied onto all the related models
meta_fields = ['source', 'election_id', 'state',]

contest_fields = meta_fields + ['start_date', 'end_date',
    'election_type', 'primary_type', 'result_type', 'special',]
candidate_fields = meta_fields + ['full_name', 'given_name', 
    'family_name', 'additional_name']
result_fields = meta_fields + ['reporting_level', 'jurisdiction',
    'votes', 'total_votes', 'vote_breakdowns']


class BaseTransform(Transform):
    """
    Base class that encapsulates shared functionality for other Maryland
    transforms.
    """

    PARTY_MAP = {
        'Both Parties': 'BOT',
        'Democratic': 'D',
        'DEM': 'D',
        'Green': 'GRE',
        'GRN': 'GRE',
        'Libertarian': 'LIB',
        'Republican': 'R',
        'REP': 'R',
        'Unaffiliated': 'UN',
        'UNF': 'UN',
    }
    """
    Map of party values as they appear in MD raw results to canonical
    abbreviations.

    In 2002, the values are party names.  Map them to abbreviations.

    From 2003 onward, the values are party abbreviations and in many
    cases match the canonical abbreviations.
    """

    district_offices = [
        "U.S. House of Representatives",
        "State Senate",
        "House of Delegates",
    ]

    def __init__(self):
        super(BaseTransform, self).__init__()
        self._office_cache = {}
        self._party_cache = {}
        self._contest_cache = {}

    def get_rawresults(self):
        # Use a non-caching queryset because otherwise we run out of memory
        return RawResult.objects.filter(state='MD').no_cache()

    def get_contest_fields(self, raw_result):
        # Resolve Office and Party related objects
        fields = self._get_fields(raw_result, contest_fields)
        if not fields['primary_type']:
            del fields['primary_type']
        fields['office'] = self._get_office(raw_result)
        fields['primary_party'] = self.get_party(raw_result, 'primary_party')
        return fields

    def _get_fields(self, raw_result, field_names):
        """
        Extract the fields from a RawResult that will be used to
        construct a related model.

        Returns a dict of fields and values that can be passed to the
        model constructor.
        """
        return { k:getattr(raw_result, k) for k in field_names } 

    def _get_office(self, raw_result):
        office_query = {
            'state': 'MD',
            'name': self._clean_office(raw_result.office),
        }

        # Handle president, where state = "US" 
        if office_query['name'] == "President":
            office_query['state'] = "US"

        if office_query['name'] in self.district_offices:
            office_query['district'] = self._strip_leading_zeros(raw_result.district)

        key = Office.make_key(**office_query)
        try:
            return self._office_cache[key]
        except KeyError:
            try:
                office = Office.objects.get(**office_query)
                # TODO: Remove this once I'm sure this always works. It should.
                assert key == office.key
                self._office_cache[key] = office
                return office
            except Office.DoesNotExist:
                print "No office matching query %s" % (office_query)
                raise

    def _clean_office(self, office):
        lc_office = office.lower()
        if "president" in lc_office:
            return "President" 
        elif "u.s. senat" in lc_office:
            return "U.S. Senate"
        elif "congress" in lc_office:
            return "U.S. House of Representatives"
        elif "state senat" in lc_office:
            # Match both "State Senate" and "State Senator"
            return "State Senate"
        elif "governor" in lc_office:
            return "Governor"

        return office

    def _strip_leading_zeros(self, val):
        return val.lstrip("0")

    def get_party(self, raw_result, attr='party'):
        party = getattr(raw_result, attr)
        if not party:
            return None

        clean_abbrev =self._clean_party(party)
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
                print "No party with abbreviation %s" % (clean_abbrev)
                raise

    def _clean_party(self, party):
        try:
            return self.PARTY_MAP[party]
        except KeyError:
            return party

    def get_candidate_fields(self, raw_result):
        year = raw_result.end_date.year
        if year == 2002:
            return self.get_candidate_fields_2002(raw_result)

        fields = self._get_fields(raw_result, candidate_fields)
        if fields['full_name'] == "Other Write-Ins":
            return fields

        name = HumanName(raw_result.full_name)
        fields['given_name'] = name.first
        fields['family_name'] = name.last
        fields['additional_name'] = name.middle
        fields['suffix'] = name.suffix
        return fields

    def get_candidate_fields_2002(self, raw_result):
        fields = self._get_fields(raw_result, candidate_fields)
        if fields['family_name'] == 'zz998':
            # Write-In
            del fields['family_name']
            del fields['given_name']
            del fields['additional_name']
            fields['full_name'] =  "Other Write-Ins"
        else:
            bits = [fields['given_name']]
            if fields['additional_name'] == '\\N':
                # Null last name
                del fields['additional_name']
            else:
                bits.append(fields['additional_name'])
            bits.append(fields['family_name'])   
            fields['full_name'] = ' '.join(bits)

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

        for rr in self.get_rawresults():
            key = self._contest_key(rr)
            if key not in seen:
                fields = self.get_contest_fields(rr)
                fields['updated'] = fields['created'] = datetime.now()
                contest = Contest(**fields)
                contests.append(contest)
                seen.add(key)

        Contest.objects.insert(contests, load_bulk=False)
        print "Created %d contests." % len(contests)

    def reverse(self):
        old = Contest.objects.filter(state='MD')
        print "\tDeleting %d previously created contests" % old.count() 
        old.delete()

    def _contest_key(self, raw_result):
        slug = raw_result.contest_slug
        return (raw_result.election_id, slug)


class CreateCandidatesTransform(BaseTransform):
    name = 'create_unique_candidates'

    def __init__(self):
        super(CreateCandidatesTransform, self).__init__()

    def __call__(self):
        candidates = []
        seen = set()

        for rr in self.get_rawresults():
            key = (rr.election_id, rr.contest_slug, rr.candidate_slug)
            if key not in seen:
                fields = self.get_candidate_fields(rr)
                fields['contest'] = self.get_contest(rr) 
                if "other" in fields['full_name'].lower():
                    if fields['full_name'] == "Other Write-Ins":
                        fields['flags'] = ['aggregate',]
                    else:
                        # As far as I can tell the value should always be
                        # "Other Write-Ins", but output a warning to let us
                        # know about some cases we may be missing.
                        logging.warn("'other' found in candidate name field"
                                "value: '%s'" % rr.full_name)
                candidate = Candidate(**fields)
                candidates.append(candidate)
                seen.add(key)

        Candidate.objects.insert(candidates, load_bulk=False)
        print "Created %d candidates." % len(candidates) 


    def reverse(self):
        old = Candidate.objects.filter(state='MD')
        print "\tDeleting %d previously created candidates" % old.count() 
        old.delete()


class CreateResultsTransform(BaseTransform): 
    name = 'create_unique_results'

    auto_reverse = True

    def __init__(self):
        super(CreateResultsTransform, self).__init__()
        self._candidate_cache = {}

    def get_rawresults(self):
        # Exclude the congressional district by county results.  We'll
        # aggregate them in a separate transform
        return super(CreateResultsTransform, self).get_rawresults()\
                .filter(reporting_level__ne='congressional_district_by_county')

    def get_results(self):
        election_ids = self.get_rawresults().distinct('election_id')
        return Result.objects.filter(election_id__in=election_ids)

    def __call__(self):
        results = self._create_results_collection() 

        for rr in self.get_rawresults():
            fields = self._get_fields(rr, result_fields)
            fields['contest'] = self.get_contest(rr)
            fields['candidate'] = self.get_candidate(rr, extra={
                'contest': fields['contest'],
            })
            fields['contest'] = fields['candidate'].contest 
            fields['raw_result'] = rr
            party = self.get_party(rr)
            if party:
                fields['party'] = party.abbrev
            fields['winner'] = self._parse_winner(rr)
            fields['jurisdiction'] = self._strip_leading_zeros(rr.jurisdiction)
            fields = self._alter_result_fields(fields, rr)
            result = Result(**fields)
            results.append(result)

        self._create_results(results)

    def _alter_result_fields(self, fields, raw_result):
        """
        Hook to do set additional or alter additional field values
        that will be passed to the Result constructor.
        """
        fields['write_in'] = self._parse_write_in(raw_result)
        fields['ocd_id'] = self._get_ocd_id(raw_result)
        return fields

    def _create_results_collection(self):
        """
        Creates the list-like object that will be used to hold the
        constructed Result instances.
        """
        return BulkInsertBuffer(Result) 

    def _create_results(self, results):
        """
        Create the Result objects in the database.
        """
        results.flush()
        print "Created %d results." % results.count()

    def reverse(self):
        old_results = self.get_results()
        print "\tDeleting %d previously loaded results" % old_results.count() 
        old_results.delete()

    def get_candidate(self, raw_result, extra={}):
        """
        Get the Candidate model for a RawResult

        Keyword arguments:

        * extra - Dictionary of extra query parameters that will
                  be used to select the candidate. 
        """
        key = (raw_result.election_id, raw_result.contest_slug,
            raw_result.candidate_slug)
        try:
            return self._candidate_cache[key]
        except KeyError:
            fields = self.get_candidate_fields(raw_result)
            fields.update(extra)
            del fields['source']
            try:
                candidate = Candidate.objects.get(**fields)
            except Candidate.DoesNotExist:
                print fields 
                raise
            self._candidate_cache[key] = candidate 
            return candidate

    def _parse_winner(self, raw_result):
        """
        Converts raw winner value into boolean
        """
        if raw_result.winner == 'Y':
            # Winner in post-2002 contest
            return True
        elif raw_result.winner == 1:
            # Winner in 2002 contest
            return True
        else:
            return False

    def _parse_write_in(self, raw_result):
        """
        Converts raw write-in value into boolean
        """
        if raw_result.write_in == 'Y':
            # Write-in in post-2002 contest
            return True
        elif raw_result.family_name == 'zz998':
            # Write-in in 2002 contest
            return True
        elif raw_result.write_in == "Write-In":
            return True
        elif raw_result.full_name == "Other Write-Ins":
            return True
        else:
            return False

    def _get_ocd_id(self, raw_result, reporting_level=None):
        """
        Returns the OCD ID for a RawResult's reporting level.

        Arguments:
        
        raw_result - the RawResult instance used to determine the OCD ID
        reporting_level - the reporting level to reflect in the OCD ID.
            Default is raw_result.reporting_level. Specifying this
            argument is useful if you want to use a RawResult's
            jurisdiction, but override the reporting level.

        """
        if reporting_level is None:
            reporting_level = raw_result.reporting_level
        juris_ocd = ocd_type_id(raw_result.jurisdiction)
        if reporting_level == "county":
            # TODO: Should jurisdiction/ocd_id be different for Baltimore City?
            return "ocd-division/country:us/state:md/county:%s" % juris_ocd 
        elif reporting_level == "state_legislative":
            return "ocd-division/country:us/state:md/sldl:%s" % juris_ocd 
        elif reporting_level == "precinct": 
            return "%s/precinct:%s" % (raw_result.county_ocd_id, juris_ocd)
        else: 
            return None


class CreateDistrictResultsTransform(CreateResultsTransform):
    """
    Aggregate results that were reported by congressional districts split
    by county.
    """

    name = 'create_district_results_from_county_splits' 

    auto_reverse = True

    def __init__(self):
        super(CreateDistrictResultsTransform, self).__init__()
        self._results_cache = {}

    def __call__(self):
        results = []

        for rr in self.get_rawresults(): 
            # We only grab the meta fields here because we're aggregating results.
            # 
            # We'll grab the votes explicitely later.
            #
            # Don't parse winner because it looks like it's reported as the
            # contest winner and not the jurisdiction winner.
            # 
            # Don't parse write-in because this case is only for primaries and
            # I'm pretty sure there aren't any write-in candidates in those
            # contests.
            fields = self._get_fields(rr, meta_fields)
            fields['candidate'] = self.get_candidate(rr)
            fields['contest'] = fields['candidate'].contest
            party = self.get_party(rr)
            if party:
                fields['party'] = party.abbrev
            fields['reporting_level'] = 'congressional_district'
            fields['jurisdiction'] = self._strip_leading_zeros(rr.reporting_district)
            fields['ocd_id'] = "ocd-division/country:us/state:md/cd:%s" % (
                ocd_type_id(fields['jurisdiction']))
          
            # Instantiate a new result for this candidate, contest and jurisdiction,
            # but only do it once.
            result, instantiated = self._get_or_instantiate_result(fields)
            if instantiated:
                results.append(result)

            # Contribute votes from this particular raw result 
            votes = result.votes if result.votes else 0
            rr_votes = rr.votes if rr.votes else 0
            votes += rr_votes
            result.votes = votes

        Result.objects.insert(results, load_bulk=False)

        print "Created %d results." % len(results)

    def get_rawresults(self):
        return RawResult.objects.filter(state='MD',
            reporting_level='congressional_district_by_county')

    def get_results(self):
        results = []
        results.extend(self._get_house_results())
        results.extend(self._get_president_results())
        return results

    def _get_house_results(self):
        return Result.objects.filter(election_id='md-2000-03-07-primary',
                    contest_slug__startswith='us-house-of-representatives',
                    reporting_level='congressional_district')

    def _get_president_results(self):
        president_contests = ['president-dem', 'president-rep']
        election_ids = ['md-2000-03-07-primary', 'md-2008-02-12-primary',
            'md-2012-04-03-primary']
        return Result.objects.filter(election_id__in=election_ids,
                contest_slug__in=president_contests,
                reporting_level='congressional_district')

    def reverse(self):
        """
        Delete result objects aggregated from results reported by congressional
        districts split by county.
        """
        count = 0
        # Delete house results
        results = self._get_house_results() 
        count += results.count()
        results.delete()

        # Delete presidential contest results
        results = self._get_president_results()
        count += results.count()
        results.delete()

        print "\tDeleted %d previously loaded results" % count 

    def _get_or_instantiate_result(self, fields):
        slug = Result.make_slug(election_id=fields['election_id'],
            contest_slug=fields['contest'].slug,
            candidate_slug=fields['candidate'].slug,
            reporting_level=fields['reporting_level'],
            jurisdiction=fields['jurisdiction'])
        try:
            return self._results_cache[slug], False
        except KeyError:
            result = Result(**fields)
            self._results_cache[slug] = result
            return result, True


class Create2000PrimaryCongressCountyResultsTransform(CreateResultsTransform):
    """
    Create county-level results for the 2000 U.S. House of Representatives
    contests.

    This is done in a separate transform to avoid confusion.  The RawResults
    for these records have a reporting_level of
    'congressional_district_by_county'.  We aggregate them to the congressional
    district level in a separate transform, but we also need to create county
    result records.  This is easy since the per-county records are unique
    by district.
    """
    name = 'create_2000_primary_congress_county_results'

    def _create_results_collection(self):
        return []

    def _alter_result_fields(self, fields, raw_result):
        fields['reporting_level'] = 'county'
        fields['ocd_id'] = self._get_ocd_id(raw_result, 'county')
        return fields

    def _create_results(self, results):
        Result.objects.insert(results, load_bulk=False)
        print "Created %d results." % len(results)

    def get_results(self):
        return Result.objects.filter(state='MD',
            reporting_level='county',
            election_id='md-2000-03-07-primary',
            contest_slug__startswith='us-house-of-representatives')

    def get_rawresults(self):
        return RawResult.objects.filter(state='MD',
            reporting_level='congressional_district_by_county',
            election_id='md-2000-03-07-primary',
            office="Representative in Congress")


class NormalizePrecinctTransform(BaseTransform):
    name = 'normalize_precinct_names'

    def get_results(self):
        return Result.objects.filter(state='MD',
            reporting_level='precinct', election_id='md-2006-11-07-general',
            source='20061107__md__general__anne_arundel__precinct.csv')

    def update_ocd_id(self, ocd_id, jurisdiction):
        ocd_id_bits = ocd_id.split('/')
        ocd_id_bits.pop()
        ocd_id_bits.append(ocd_type_id("precinct:%s" % jurisdiction))
        return '/'.join(ocd_id_bits)

    def __call__(self):
        for result in self.get_results():
            district, precinct = result.jurisdiction.split('-')
            result.jurisdiction = "%s-%s" % (district,
                    precinct.zfill(3))
            result.ocd_id = self.update_ocd_id(result.ocd_id,
                result.jurisdiction)
            result.save()

    def reverse(self):
        for result in self.get_results():
            district, precinct = result.jurisdiction.split('-')
            result.jurisdiction = "%s-%s" % (district,
                    precinct.lstrip('0'))
            result.ocd_id = self.update_ocd_id(result.ocd_id,
                result.jurisdiction)
            result.save()


class RemoveBaltimoreCityComptroller(BaseTransform):
    """
    Remove Baltimore City comptroller results.

    Maryland election results use the string "Comptroller" for both the 
    state comptroller and the Baltimore City Comptroller.  We're only
    interested in the state comptroller.

    """
    name = 'remove_baltimore_city_comptroller'

    def __call__(self):
        election_id = 'md-2004-11-02-general'
        office = Office.objects.get(state='MD', name='Comptroller')
        Contest.objects.filter(election_id=election_id, office=office).delete()
        Candidate.objects.filter(election_id=election_id,
            contest_slug='comptroller').delete()
        Result.objects.filter(election_id=election_id,
            contest_slug='comptroller').delete()


class CombineUncommittedPresStateLegislativeResults(BaseTransform):
    """
    Combine "Uncommitted to Any Presidential Candidate" results.
    
    In the 2008 Democratic primary, in the results aggregated at the State
    Legislative level, there are multiple rows for the
    "Uncommitted to Any Presidential Candidate" pseudo-candidate, with empty
    values for many of the state legislative district.  There appears to be
    one row per county.  Most of these entries are empty.

    Combine these into a single result per state legislative district. 
    """
    name = 'combine_uncommitted_pres_state_leg_results'

    def __call__(self):
        results = Result.objects.filter(election_id='md-2008-02-12-primary',
            reporting_level='state_legislative',
            contest_slug='president-d',
            candidate_slug='uncommitted-to-any-presidential-candidate')
        districts = results.distinct('jurisdiction')
        assert len(districts) == 65
        for district in districts:
            district_results = results.filter(jurisdiction=district)
            assert district_results.count() == 24 
            total_votes = 0
            # Save the first result.  We'll use this for the combined results
            first_result = district_results[0]
            for result in district_results:
                total_votes += result.votes
            assert total_votes != 0
            first_result.votes = total_votes
            first_result.save()
            district_results.filter(id__ne=first_result.id).delete()


def add_precinct_result_note():
    """
    Add a note explaining that precinct-level results only contain election
    night totals.
    """
    note = "Value of votes field contains only election night vote totals."
    Result.objects.filter(reporting_level='precinct').update(notes=note)


# TODO: When should we create a Person

#def standardize_office_and_district():
#    pass

#def clean_vote_counts():
    #pass

registry.register('md', CreateContestsTransform)
registry.register('md', CreateCandidatesTransform)
registry.register('md', CreateResultsTransform)
registry.register('md', CreateDistrictResultsTransform)
registry.register('md', Create2000PrimaryCongressCountyResultsTransform)
registry.register('md', NormalizePrecinctTransform,
    [validate_precinct_names_normalized])
registry.register('md', RemoveBaltimoreCityComptroller,
    [validate_no_baltimore_city_comptroller])
registry.register('md', CombineUncommittedPresStateLegislativeResults,
    [validate_uncommitted_primary_state_legislative_results])
registry.register('md', add_precinct_result_note)
#registry.register('md', standardize_office_and_district)
#registry.register('md', clean_vote_counts)
