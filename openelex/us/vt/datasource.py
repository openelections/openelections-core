"""
In VT, we have to search for elections on the vermont secretary of state website.

We will be given some election id, we can then query election results with:
    http://vtelectionarchive.sec.state.vt.us/elections/download/%election-id%/precincts_include:0/
    or
    http://vtelectionarchive.sec.state.vt.us/elections/download/%election-id%/precincts_include:1/

To run mappings from a shell:

    openelex datasource.mappings -s vt

"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
import re
import urllib.request, urllib.error, urllib.parse
from bs4 import BeautifulSoup
from string import Template
from multiprocessing.pool import ThreadPool
from datetime import datetime

from openelex.lib import format_date, standardized_filename
from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        print((str(datetime.now()), "mappings begin"))
        """Return array of dicts  containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in list(self.elections(year).items()):
            mappings.extend(self._build_metadata(yr, elecs))
        print((str(datetime.now()), "mappings end"))
        return mappings

    def target_urls(self, year=None):
        "Get list of source data urls, optionally filtered by year"
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], item['raw_url'])
                for item in self.mappings(year)]


    # PRIVATE METHODS
    def _races_by_type(self, elections):
        "Filter races by type and add election slug"
        races = {
          'special': None,
        }
        for elec in elections:
            rtype = self._race_type(elec)
            elec['slug'] = self._election_slug(elec)
            races[rtype] = elec
        return races['general'], races['primary'], races['special']

    def _race_type(self, election):
        if election['special']:
            return 'special'

        return election['race_type'].lower()

    def _jurisdictions(self):
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['url_name'] != ""]
        return mappings
    def _jurisdictionOcdMap(self, _patern):
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if _patern in x['ocd_id']]
        return mappings
    def _jurisdictionOcdMapForStateSenate(self):
        return self._jurisdictionOcdMap("sldu")
    def _jurisdictionOcdMapForStateRep(self):
        return self._jurisdictionOcdMap("sldl")
    def _jurisdictionOcdMapForStateWide(self):
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['fips'] == "50"]
        return mappings

    def _build_metadata(self, year, elections):
        year_int = int(year)
        meta = []
        meta += self._state_leg_meta(year, elections, self.StateWideOfficeId, self._jurisdictionOcdMapForStateWide())
        meta += self._state_leg_meta(year, elections, [self.STATE_SENATE_ELEC_OFFICE_ID], self._jurisdictionOcdMapForStateSenate())
        meta += self._state_leg_meta(year, elections, [self.STATE_REP_ELEC_OFFICE_ID], self._jurisdictionOcdMapForStateRep())
        return meta

    #US Races
    PRESIDENT_ELEC_OFFICE_ID = 1
    US_SENATE_ELEC_OFFICE_ID = 6
    US_HOUSE_ELEC_OFFICE_ID = 5
    # State wide sate races
    STATE_GOV_ELEC_OFFICE_ID = 3
    STATE_LT_GOV_ELEC_OFFICE_ID = 4
    STATE_TREASURER_ELEC_OFFICE_ID = 53
    STATE_SEC_OF_STATE_ELEC_OFFICE_ID = 44
    STATE_AUDITOR_ELEC_OFFICE_ID = 13
    STATE_ATTORNEY_GEN_ELEC_OFFICE_ID = 12
    STATE_TREASURER_ELEC_OFFICE_ID = 53
    # State office per district
    STATE_SENATE_ELEC_OFFICE_ID = 9
    STATE_REP_ELEC_OFFICE_ID = 8
    #TODO County officials.

    StateWideOfficeId = [
        PRESIDENT_ELEC_OFFICE_ID,
        US_SENATE_ELEC_OFFICE_ID,
        US_HOUSE_ELEC_OFFICE_ID,
        STATE_GOV_ELEC_OFFICE_ID,
        STATE_LT_GOV_ELEC_OFFICE_ID,
        STATE_TREASURER_ELEC_OFFICE_ID,
        STATE_SEC_OF_STATE_ELEC_OFFICE_ID,
        STATE_AUDITOR_ELEC_OFFICE_ID,
        STATE_ATTORNEY_GEN_ELEC_OFFICE_ID,
    ]

    OfficeIdToOfficeNameMap = {
        PRESIDENT_ELEC_OFFICE_ID:           "president",
        US_SENATE_ELEC_OFFICE_ID:           "senate",
        US_HOUSE_ELEC_OFFICE_ID:            "house",
        STATE_GOV_ELEC_OFFICE_ID:           "governor",
        STATE_LT_GOV_ELEC_OFFICE_ID:        "lieutenant_governor",
        STATE_TREASURER_ELEC_OFFICE_ID:     "treasurer",
        STATE_SEC_OF_STATE_ELEC_OFFICE_ID:  "secretary_of_state",
        STATE_AUDITOR_ELEC_OFFICE_ID:       "auditor",
        STATE_ATTORNEY_GEN_ELEC_OFFICE_ID:  "attorney_general",
        STATE_SENATE_ELEC_OFFICE_ID:        "state_senate",
        STATE_REP_ELEC_OFFICE_ID:           "state_house"
    }

    search_url_expr = "http://vtelectionarchive.sec.state.vt.us/elections/search/year_from:$year/year_to:$year/office_id:$office_id"
    electionViewUrl = "http://vtelectionarchive.sec.state.vt.us/elections/download/$electionId/precincts_include:$isPrecinct/"
    def _getElectionViewUrl(self, elecId, isPrecinct):
        return Template(self.electionViewUrl).substitute(electionId=elecId, isPrecinct=(1 if isPrecinct else 0))

    def _getElectionList(self, year, officeId):
        payload = []
        search_url = Template(self.search_url_expr).substitute(year=year, office_id=officeId)
        response = urllib.request.urlopen(search_url)
        html_doc = response.read()

        # load election from search results.
        soup = BeautifulSoup(html_doc, 'html.parser')
        resultTableRows = soup.find_all(id=re.compile("election-id-*"))
        for resRow in resultTableRows:
            elemId = resRow.get('id')
            elemElectId = elemId.split('-')[-1]
            resCols = resRow.find_all('td')
            year, office, district, stage = resCols[0].text,resCols[1].text,resCols[2].text,resCols[3].text
            payload.append({'officeId': officeId,'id': elemElectId, 'year': year, 'office': office, 'district': district, 'stage': stage})
        return officeId, payload
    def _findElecByType(self, elecDict, str):
        return [e for e in elecDict if e['stage'] == str]

    def _officeNameFromId(self, officeId):
        return self.OfficeIdToOfficeNameMap[officeId]


    def _state_leg_meta(self, year, elections, officeIds, jurisdictions):
        year_int = int(year)
        payload = []
        general, primary, special = self._races_by_type(elections)

        electByPosition = ThreadPool(20).imap_unordered( lambda x: self._getElectionList(year_int, x), officeIds)

        for positionId, elecDict in electByPosition:
            for precinct_val in (True, False):
                if general:
                    genElections = self._findElecByType(elecDict, "General Election")
                    for genElect in genElections:
                        payload.append(self._generatedOneStateLegElectionMetaData(genElect, general, None, jurisdictions, precinct_val))

                if primary:
                    for party in ['Democratic', 'Republican', 'Progressive', 'Liberty Union']:
                        electionType = party + " Primary"
                        primElections = self._findElecByType(elecDict, electionType)
                        for primElect in primElections:
                            payload.append(self._generatedOneStateLegElectionMetaData(primElect, primary, party, jurisdictions, precinct_val))

        return payload


    def _generatedOneStateLegElectionMetaData(self, elecVt, election, primaryParty, jurisdictions, isPrecinct):
        raceType = "primary" if primaryParty else "general"
        office = self._officeNameFromId(elecVt['officeId'])

        jurisdiction = elecVt['district']
        if jurisdiction == "Statewide":
            jurisdiction = "Vermont"

        generatedFileName = standardized_filename('vt', election['start_date'], '.csv',
            party=primaryParty,
            race_type=raceType,
            reporting_level= "precinct" if isPrecinct  else None,
            jurisdiction=jurisdiction,
            office=office)

        meta = {
            'name'                  : jurisdiction,
            'raw_url'               : self._getElectionViewUrl(elecVt['id'], isPrecinct),
            'generated_filename'    : generatedFileName,
            'election'              : election['slug'],
            # specific for VT
            'isPrecinct'    : isPrecinct,
            'office'        : office,
            'isPrimary'     : raceType == "general",
            'primaryParty'  : primaryParty,
            'officeDistrict': jurisdiction,
            }
        return meta
