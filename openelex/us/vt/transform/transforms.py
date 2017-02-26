from datetime import datetime
import logging
import time

from nameparser import HumanName

from openelex.base.transform import Transform
from openelex.models import Candidate, Contest, Office, Party, RawResult, Result
from openelex.lib.text import ocd_type_id
from openelex.lib.insertbuffer import BulkInsertBuffer

# if not os.path.isdir("logs"):
#     os.makedirs("logs")
# logging.basicConfig(filename=time.strftime("logs/%Y%m%d-%H%M%S-tranfor.log"),level=logging.WARNING)

# Lists of fields on RawResult that are contributed to the canonical
# models.  Maybe this makes more sense to be in the model.

# Meta fields get copied onto all the related models
meta_fields = ['source', 'election_id', 'state',]

contestKeyFields = ['election_id', 'office', 'district', 'primary_party']
contest_fields = meta_fields + ['start_date', 'end_date',
    'election_type', 'primary_type', 'result_type', 'special',]
candidateKeyFields = contestKeyFields + ['full_name']
candidate_fields = meta_fields + ['full_name', 'given_name',
    'family_name', 'additional_name']
result_fields = meta_fields + ['reporting_level', 'jurisdiction',
    'votes']

class BaseTransform(Transform):
    """
    Base class that encapsulates shared functionality for other Vermont
    transforms.
    """
    district_offices = [
        "State Senate",
        "House of Representatives",
    ]

    PARTY_MAP = {
        'both parties': 'BOT',
        'democratic': 'D',
        'green': 'GRE',
        'libertarian': 'LIB',
        'republican': 'R',
        'unaffiliated': 'UN',
        'independent': 'IND',
        'energy independence': 'IND',
        'anti-bushist candidate': 'IND',
        'impeach bush now': 'IND',
        'restore justice-freedom': 'IND',
        'cheap renewable energy': 'IND',
        'liberty union': "LUP",
        'liberty union/progressive': 'LUP', # Jane Newton in 2002 was both LUP and VPP (Vermont prog. party)
        'liberty union/democratic': 'LUP', #Pete Diamondstone 2000,
        'unf': 'UN',
        'votekiss':'VKS',
        'peace and prosperity':'PEP',
        'justice': 'JUS',
        'socialist': 'SUS',
        'u.s. marijuana': 'USM',
        'marijuana': 'USM',
        'Make Marijuana Legal': 'USM',
        'constitution': 'CON',
        'progressive': 'PRO',
        'natural law': 'NLP',
        'socialist workers': 'SWP',
        'socialism and liberation': 'SLP',
        'we the people': 'WTP',
        'Vermont Grassroots': 'VGP',
        'progressive/green': 'VPP', # Ralph Nader 2000 was Green party nominee in the US, but Progressive party nominee in VT.
        'democratic/republican': 'D', # Peter Welch in 2008 was both party nominee. In general, he was mostly Democrat.
    }
    def __init__(self):
        super(BaseTransform, self).__init__()
        self._office_cache = {}
        self._party_cache = {}
        self._contest_cache = {}

    def get_rawresults(self):
        # Use a non-caching queryset because otherwise we run out of memory
        return RawResult.objects.filter(state='VT').no_cache()

    def get_contest_fields(self, raw_resultDict):
        # Resolve Office and Party related objects
        fields = self._get_fields(raw_resultDict, contest_fields)

        if not fields['primary_type']:
            del fields['primary_type']
        fields['office'] = self._get_office(raw_resultDict)
        # print("Before get party: ", fields)
        fields['primary_party'] = self.get_party(raw_resultDict, 'primary_party')
        # print("get_contest_fields: ", fields)
        return fields

    def _get_fields(self, raw_resultDict, field_names):
        """
        Extract the fields from a RawResult that will be used to
        construct a related model.

        Returns a dict of fields and values that can be passed to the
        model constructor.
        """
        fields = {k: raw_resultDict.get(k, None) for k in field_names}
        return fields

    def _contest_key(self, raw_resultDict):
        return self._generateKeyFromFields(raw_resultDict, contestKeyFields)

    def _candidate_key(self, raw_resultDict):
        return self._generateKeyFromFields(raw_resultDict, candidateKeyFields)

    def _generateKeyFromFields(self, raw_resultDict, keyFields):
        return "_".join([raw_resultDict.get(k, "") for k in keyFields])
    def get_contest(self, raw_resultDict):
        """
        Returns the Contest model instance for a given RawResult.

        Caches the result in memory to reduce the number of calls to the
        datastore.
        """
        key = self._contest_key(raw_resultDict)
        try:
            return self._contest_cache[key]
        except KeyError:
            fields = self.get_contest_fields(raw_resultDict)
            fields.pop('source')
            try:
                contest = Contest.objects.get(**fields)
            except Exception:
                print(fields)
                raise
            self._contest_cache[key] = contest
            return contest

    def _get_office(self, raw_resultDict):
        office_query = {
            'state': 'VT',
            'name': self._clean_office(raw_resultDict.get('office', "")),
        }

        # Handle president, where state = "US"
        if office_query['name'] == "President":
            office_query['state'] = "US"

        if office_query['name'] in self.district_offices:
            jurisdiction = self._cleanDistrict(raw_resultDict.get('district',""))
            office_query['district'] = jurisdiction

        key = Office.make_key(**office_query)
        try:
            return self._office_cache[key]
        except KeyError:
            try:
                print (office_query)
                office = Office.objects.get(**office_query)
                # TODO: Remove this once I'm sure this always works. It should.
                assert key == office.key
                self._office_cache[key] = office
                return office
            except Office.DoesNotExist:
                print (raw_resultDict.get('office', ""))
                print (self._clean_office(raw_resultDict.get('office', "")))
                print "No office matching query %s" % (office_query)
                raise

    def _cleanDistrict(self, district):
        if district.endswith(" County"):
            district = district[:-7]
        if district == "Windham-Bennington 1":
            district = "Windham-Bennington"
        if district == "Windham-Bennington-Windsor 1" or district == "Windham Bennington Windosr 1":
            district = "Windham-Bennington-Windsor"
        district.replace("Chittdenden", Chittenden)
        return district


    def _clean_office(self, office):
        lc_office = office.lower()
        if "president" in lc_office:
            return "President"
        elif "state_senate" in lc_office:
            return "State Senate"
        elif "state_house" in lc_office:
            return "House of Representatives"
        elif "senate" in lc_office:
            return "U.S. Senate"
        elif "house" in lc_office:
            return "U.S. House of Representatives"
        elif "lieutenant_governor" in lc_office:
            return "Lieutenant Governor"
        elif "governor" in lc_office:
            return "Governor"
        elif "treasurer" in lc_office:
            return "Treasurer"
        elif "secretary_of_state" in lc_office:
            return "Secretary of State"
        elif "auditor" in lc_office:
            return "Auditor"
        elif "attorney_general" in lc_office:
            return "Attorney General"

        return office

    def get_party(self, raw_resultDict, attr='party'):
        party = raw_resultDict.get(attr, None)
        if not party:
            return None

        clean_abbrev =self._clean_party(party)
        if not clean_abbrev:
            if party == "Vermont Localist":
                clean_abbrev = self._cleanPartyVermontLocalist(raw_resultDict)

        if not clean_abbrev:
            return None

        try:
            return self._party_cache[clean_abbrev]
        except KeyError:
            try:
                print('getting party for ', party, clean_abbrev)
                party = Party.objects.get(abbrev=clean_abbrev)
                self._party_cache[clean_abbrev] = party
                return party
            except Party.DoesNotExist:
                print(raw_resultDict)
                print "No party with abbreviation %s" % (clean_abbrev)
                raise

    def _clean_party(self, party):
        try:
            return self.PARTY_MAP[party.lower()]
        except KeyError:
            return None

    def _cleanPartyVermontLocalist(self, raw_resultDict):
        if raw_resultDict['full_name'] == "Pat Buchanan":
            return "REF"
        if raw_resultDict['full_name'] == "Benjamin Clarke":
            return "IND"
        return ""

    def get_candidate_fields(self, raw_resultDict):
        fields = self._get_fields(raw_resultDict, candidate_fields)
        if fields['full_name'] == "Write-Ins":
            return fields

        name = HumanName(raw_resultDict['full_name'])
        fields['given_name'] = name.first
        fields['family_name'] = name.last
        fields['additional_name'] = name.middle
        fields['suffix'] = name.suffix
        return fields

    def _generateAggregateDict(self, keyFields):
        return {"$concat" : [{ "$ifNull": [ "$"+k, "" ] } for k in keyFields]},


def logResult(_prefix, _d):
    logging.info("%s: %s", _prefix, str(_d))


class CreateContestsTransform(BaseTransform):
    name = 'create_unique_contests'
    auto_reverse = True

    def __call__(self):
        print(str(datetime.now()), "CreateContestsTransform begin")
        contests = []

        contestElem = {c: 1 for c in contest_fields}
        contestElem.update({
            "contestKey":
                self._generateAggregateDict(contestKeyFields),
            "rr" : "$$ROOT"
        })
        contestElemGroups = {c: {"$first": "$"+c} for c in contest_fields}
        contestElemGroups['_id'] = "$contestKey"
        contestElemGroups['rr'] = {"$first" : "$rr"}
        pipeline = [
            {"$match": {"state":'VT'} },
            {"$project": contestElem },
            {"$group"  : contestElemGroups }
            ]

        aggregatedResults = RawResult.objects.aggregate(*pipeline)
        for rr in aggregatedResults:
            logResult("creating contest: ", rr)
            fields = self.get_contest_fields(rr['rr'])
            fields['updated'] = fields['created'] = datetime.now()
            contest = Contest(**fields)
            contests.append(contest)

        if len(contests) > 0:
            Contest.objects.insert(contests, load_bulk=False)
        print (str(datetime.now()), "Created %d contests." % len(contests))

    def reverse(self):
        old = Contest.objects.filter(state='VT')
        print "\tDeleting %d previously created contests" % old.count()
        old.delete()




class CreateCandidates(BaseTransform):
    name = 'create_candidates'
    auto_reverse = True

    def __call__(self):
        print(str(datetime.now()), "CreateCandidates begin")
        candidates = []

        candidateElem = {c: 1 for c in candidate_fields}
        candidateElem.update({
            "candidateKey":
                self._generateAggregateDict(candidateKeyFields),
            "rr" : "$$ROOT"
        })
        candidateElemGroups = {c: {"$first": "$"+c} for c in candidate_fields}
        candidateElemGroups['_id'] = "$candidateKey"
        candidateElemGroups['rr'] = {"$first" : "$rr"}
        pipeline = [
            {"$match": {"state":'VT'} },
            {"$project": candidateElem },
            {"$group"  : candidateElemGroups }
            ]

        print ("pipeline", pipeline)
        aggregatedResults = RawResult.objects.aggregate(*pipeline)

        for rr in aggregatedResults:
            logResult("creating candidate: ", rr)
            fields = self.get_candidate_fields(rr['rr'])
            fields['contest'] = self.get_contest(rr['rr'])
            if fields['full_name'] == 'Write-Ins':
                fields['flags'] = ['aggregate',]

            candidate = Candidate(**fields)
            candidates.append(candidate)

        if len(candidates) > 0:
            Candidate.objects.insert(candidates, load_bulk=False)
        print (str(datetime.now()), "Created %d candidates." % len(candidates))


    def reverse(self):
        old = Candidate.objects.filter(state='VT')
        print "\tDeleting %d previously created candidates" % old.count()
        old.delete()

class CreateResultsTransform(BaseTransform):
    name = 'create_unique_results'

    auto_reverse = True

    def __init__(self):
        super(CreateResultsTransform, self).__init__()
        self._candidate_cache = {}

    def get_results(self):
        election_ids = self.get_rawresults().distinct('election_id')
        return Result.objects.filter(election_id__in=election_ids)

    def __call__(self):
        print(str(datetime.now()), "CreateResultsTransform begin")
        results = self._create_results_collection()

        pipeline = [{"$match": {"state":'VT'} }]

        aggregatedResults = RawResult.objects.aggregate(*pipeline)

        # for rr in aggregatedResults:
        #     print (rr)
        #     fields = self.get_candidate_fields(rr['rr'])
        #     fields['contest'] = self.get_contest(rr['rr'])
        #     if fields['full_name'] == 'Write-Ins':
        #         fields['flags'] = ['aggregate',]


        for rr in aggregatedResults:
            logResult("creating result: ", rr)
            fields = self._get_fields(rr, result_fields)
            fields['contest'] = self.get_contest(rr)
            fields['candidate'] = self.get_candidate(rr, extra={'contest': fields['contest'],})
            fields['contest'] = fields['candidate'].contest
            # fields['raw_result'] = rr
            jurisdiction = rr.get('jurisdiction', None)
            parent_jurisdiction = rr.get('parent_jurisdiction', None)

            if fields['candidate']['full_name'] == 'Write-Ins':
                fields['write_in'] = True

            party = self.get_party(rr)
            if party:
                fields['party'] = party.abbrev
            fields['jurisdiction'] = jurisdiction
            # if precinct is same as town.
            if fields['reporting_level'] == "precinct":
                if not jurisdiction or jurisdiction == '':
                    fields['jurisdiction'] = parent_jurisdiction
                    fields['ocd_id'] = "ocd-division/country:us/state:vt/place:%s" % ocd_type_id(parent_jurisdiction)
                else:
                    fields['jurisdiction'] = jurisdiction
                    fields['ocd_id'] = "ocd-division/country:us/state:vt/place:%s/precinct:%s" % (ocd_type_id(parent_jurisdiction), ocd_type_id(jurisdiction))
            else:
                fields['jurisdiction'] = jurisdiction
                fields['ocd_id'] = "ocd-division/country:us/state:vt/place:%s" % ocd_type_id(jurisdiction)
            fields = self._alter_result_fields(fields, rr)
            result = Result(**fields)
            results.append(result)

        self._create_results(results)
        print (str(datetime.now()), "Created %d results." % results.count())

    def reverse(self):
        old_results = self.get_results()
        print "\tDeleting %d previously loaded results" % old_results.count()
        old_results.delete()


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


    def _alter_result_fields(self, fields, raw_resultDict):
        """
        Hook to do set additional or alter additional field values
        that will be passed to the Result constructor.
        """
        fields['write_in'] = False
        return fields

    def get_candidate(self, raw_resultDict, extra={}):
        """
        Get the Candidate model for a RawResult

        Keyword arguments:

        * extra - Dictionary of extra query parameters that will
                  be used to select the candidate.
        """
        key = self._candidate_key(raw_resultDict)
        try:
            return self._candidate_cache[key]
        except KeyError:
            fields = self.get_candidate_fields(raw_resultDict)
            fields.update(extra)
            del fields['source']
            try:
                print(fields)
                candidate = Candidate.objects.get(**fields)
            except Candidate.DoesNotExist:
                print fields
                raise
            self._candidate_cache[key] = candidate
            return candidate
