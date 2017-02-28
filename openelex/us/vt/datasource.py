"""
In VT, we have to search for elections on the vermont secretary of state website.

We will be given some election id, we can then query election results with:
    http://vtelectionarchive.sec.state.vt.us/elections/download/%election-id%/precincts_include:0/
    or
    http://vtelectionarchive.sec.state.vt.us/elections/download/%election-id%/precincts_include:1/

To run mappings from a shell:

    openelex datasource.mappings -s md

"""
import re
import urllib2
from bs4 import BeautifulSoup
from string import Template
from multiprocessing.pool import ThreadPool

from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):

    base_url = "http://www.elections.state.md.us/elections/%(year)s/election_data/"

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts  containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
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

    def _build_metadata(self, year, elections):
        year_int = int(year)
        meta = []
        meta += self._state_leg_meta(year, elections)
        return meta

    PRESIDENT_ELEC_OFFICE_ID = 1
    US_SENATE_ELEC_OFFICE_ID = 6
    US_HOUSE_ELEC_OFFICE_ID = 5
    STATE_GOV_ELEC_OFFICE_ID = 3
    STATE_LT_GOV_ELEC_OFFICE_ID = 4
    STATE_SENATE_ELEC_OFFICE_ID = 9
    STATE_REP_ELEC_OFFICE_ID = 8
    search_url_expr = "http://vtelectionarchive.sec.state.vt.us/elections/search/year_from:$year/year_to:$year/office_id:$office_id"
    electionViewUrl = "http://vtelectionarchive.sec.state.vt.us/elections/download/$electionId/precincts_include:$isPrecinct/"
    def _getElectionViewUrl(self, elecId, isPrecinct):
        return Template(self.electionViewUrl).substitute(electionId=elecId, isPrecinct=(1 if isPrecinct else 0))

    def _getElectionList(self, year, officeId):
        payload = []
        search_url = Template(self.search_url_expr).substitute(year=year, office_id=officeId)
        response = urllib2.urlopen(search_url)
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
        return filter(lambda e: e['stage'] == str, elecDict)

    def _officeNameFromId(self, officeId):
        nameMap = {
            self.PRESIDENT_ELEC_OFFICE_ID: "president",
            self.US_SENATE_ELEC_OFFICE_ID: "senate",
            self.US_HOUSE_ELEC_OFFICE_ID: "house",
            self.STATE_GOV_ELEC_OFFICE_ID: "state_gov",
            self.STATE_LT_GOV_ELEC_OFFICE_ID: "state_ltgov",
            self.STATE_SENATE_ELEC_OFFICE_ID: "state_senate",
            self.STATE_REP_ELEC_OFFICE_ID: "state_house"
        }
        return nameMap[officeId]
    def _generatedNameForElectionStateLeg(self, elecVt, election, isPrecinct):
        officeId = elecVt['officeId']
        bits = [
            election['start_date'].replace('-',''),
            self.state,
        ]

        primaryPattern = re.compile(r"(?P<party>Democratic|Republican) Primary", re.IGNORECASE )
        primarySearchRes = re.search(primaryPattern, elecVt['stage'])
        isPrimary = False
        primaryParty = ""
        if primarySearchRes:
            isPrimary = True
            matches = primarySearchRes.groupdict()
            primaryParty = matches['party'].lower()

        if isPrimary:
            bits.append(primaryParty)
            bits.append("primary")
        else:
            bits.append("general")

        bits.append(self._officeNameFromId(officeId))

        if isPrecinct:
            bits.append("precinct")

        filename = "__".join(bits) + '.csv'
        return filename

    def _generatedOneStateLegElectionMetaData(self, elecVt, election, isPrecinct):
        meta = {
            'ocd_id': 'ocd-division/country:us/state:vt',
            'name': 'Vermont',
        }
        meta.update({
            'raw_url': self._getElectionViewUrl(elecVt['id'], isPrecinct),
            'generated_filename': self._generatedNameForElectionStateLeg(elecVt, election, isPrecinct),
            'election': election['slug']
        })
        return meta


    def _state_leg_meta(self, year, elections):
        year_int = int(year)
        payload = []
        general, primary, special = self._races_by_type(elections)

        AllStatePositions = [
            self.PRESIDENT_ELEC_OFFICE_ID,
            self.US_SENATE_ELEC_OFFICE_ID,
            self.US_HOUSE_ELEC_OFFICE_ID,
            self.STATE_GOV_ELEC_OFFICE_ID,
            self.STATE_LT_GOV_ELEC_OFFICE_ID]

        electByPosition = ThreadPool(20).imap_unordered( lambda x: self._getElectionList(year_int, x), AllStatePositions)
        electionResMapping = {}
        for officeId, r in electByPosition:
            electionResMapping[officeId] = r

        for positionId in AllStatePositions:
            presElecs = electionResMapping[positionId]

            for precinct_val in (True, False):
                if general:
                    genElect = self._findElecByType(presElecs, "General Election")
                    if len(genElect) > 0:
                        payload.append(self._generatedOneStateLegElectionMetaData(genElect[0], general, precinct_val))
                        # print(genElect)

                if primary:
                    for party in ['Democratic', 'Republican']:
                        electionType = party + " Primary"
                        dpElect = self._findElecByType(presElecs, electionType)
                        if len(dpElect) > 0:
                            payload.append(self._generatedOneStateLegElectionMetaData(dpElect[0], primary, precinct_val))
                            # print(dpElect)

        return payload

    def _jurisdictions(self):
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['url_name'] != ""]
        return mappings
