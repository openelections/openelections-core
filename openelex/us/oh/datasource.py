"""
Standardize names of data files on Ohio Secretary of State.

File-name conventions on OH site vary widely according to election, but typically there is a single precinct file, a race-wide (county) file
and additional files for absentee and provisional ballots. Earlier election results are in HTML and have no precinct files. This example is from
the 2012 election:

    general election
        precinct:            yyyymmddprecinct.xlsx
        county:              FinalResults.xlsx
        provisional:         provisional.xlsx
        absentee:            absentee.xlsx

    primary election
        precinct:            2012precinct.xlsx
        county:              [per-race csv files]
        provisional:         provisional.xlsx
        absentee:            absentee.xlsx

    Exceptions: 2000 & 2002 are HTML pages with no precinct-level results.

The elections object created from the Dashboard API includes a portal link to the main page of results (needed for scraping results links) and a
direct link to the most detailed data for that election (precinct-level, if available, for general and primary elections or county level otherwise).
If precinct-level results file is available, grab that. If not, run the _url_paths function to load details about the location and scope of HTML (aspx)
files. Use the path attribute and the base_url to construct the full raw results URLs.
"""
from future import standard_library
standard_library.install_aliases()
import os
from os.path import join
import json
import unicodecsv
import urllib.parse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in list(self.elections(year).items()):
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        "Get list of source data urls, optionally filtered by year"
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], item['raw_url'])
                for item in self.mappings(year)]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        year_int = int(year)
        precinct_elections = [e for e in elections if e['precinct_level'] == True]
        other_elections = [e for e in elections if e['precinct_level'] == False]
        if precinct_elections:
            meta = self._precinct_meta(year, precinct_elections)
        if other_elections:
            if not meta:
                meta = []
            for election in other_elections:
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    meta.append({
                        "generated_filename":
                        self._generate_office_filename(election['direct_links'][0], election['start_date'], election['race_type'], result),
                        "raw_url": self._build_raw_url(year, result['path']),
                        "ocd_id": 'ocd-division/country:us/state:oh',
                        "name": 'Ohio',
                        "election": election['slug']
                    })
        return meta

    def _build_raw_url(self, year, path):
        return "http://www.sos.state.oh.us/sos/elections/Research/electResultsMain/%sElectionsResults/%s" % (year, path)

    def _precinct_meta(self, year, elections):
        payload = []
        meta = {
            'ocd_id': 'ocd-division/country:us/state:oh',
            'name': 'Ohio',
        }

        try:
            general = [e for e in elections if e['race_type'] == 'general'][0]
        except:
            general = None

        try:
            primary = [e for e in elections if e['race_type'] == 'primary'][0]
        except:
            primary = None

        if general:
            # Add General meta to payload
            general_url = [g for g in general['direct_links'] if 'precinct' in g][0]
            general_filename = self._generate_precinct_filename(general_url, general['start_date'], 'general')
            gen_meta = meta.copy()
            gen_meta.update({
                'raw_url': general_url,
                'generated_filename': general_filename,
                'election': general['slug']
            })
            payload.append(gen_meta)

            general_county_url = [g for g in general['direct_links'] if 'esults' in g][0]
            general_county_filename = self._generate_county_filename(general_county_url, general['start_date'], 'general', None)
            gen_county_meta = meta.copy()
            gen_county_meta.update({
                'raw_url': general_county_url,
                'generated_filename': general_county_filename,
                'election': general['slug']
            })
            payload.append(gen_county_meta)

        # Add Primary meta to payload
        if primary and int(year) > 2000:
            pri_meta = meta.copy()
            primary_url = [p for p in primary['direct_links'] if 'precinct' in p][0]
            primary_filename = self._generate_precinct_filename(primary_url, primary['start_date'], 'primary')
            pri_meta.update({
                'raw_url': primary_url,
                'generated_filename': primary_filename,
                'election': primary['slug']
            })
            payload.append(pri_meta)

            # check url_paths for county files
            results = [x for x in self._url_paths() if x['date'] == primary['start_date']]
            for result in results:
                payload.append({
                    "generated_filename":
                    self._generate_county_filename(result['url'], result['date'], 'primary', result['party']),
                    "raw_url": result['url'],
                    "ocd_id": 'ocd-division/country:us/state:oh',
                    "name": 'Ohio',
                    "election": primary['slug']
                })

        return payload

    def _generate_precinct_filename(self, url, start_date, election_type):
        # example: 20121106__oh__general__precinct.xlsx
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            'precinct'
        ]
        path = urllib.parse.urlparse(url).path
        ext = os.path.splitext(path)[1]
        name = "__".join(bits)+ ext
        return name

    def _generate_county_filename(self, url, start_date, election_type, party):
        # example: 20121106__oh__general__county.xlsx
        if party:
            election_type = party+"__"+election_type
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            'county'
        ]
        path = urllib.parse.urlparse(url).path
        ext = os.path.splitext(path)[1]
        name = "__".join(bits)+ ext
        return name

    def _generate_office_filename(self, url, start_date, election_type, result):
        # example: 20021105__oh__general__gov.aspx
        if result['district'] == '':
            office = result['office']
        else:
            office = result['office'] + '__' + result['district']
        if result['special'] == '1':
            election_type = 'special__' + election_type
        if result['race_type'] == 'general':
            bits = [
                start_date.replace('-',''),
                self.state.lower(),
                election_type,
                office
            ]
        else:
            bits = [
                start_date.replace('-',''),
                self.state.lower(),
                result['party'],
                election_type,
                office
            ]
        path = urllib.parse.urlparse(url).path
        name = "__".join(bits)+'.html'
        return name

    def _jurisdictions(self):
        """Ohio counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['results_name'] != ""]
        return mappings
