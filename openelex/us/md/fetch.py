"""
Standardize names of data files on Maryland State Board of Elections and 
save to mappings/filenames.json

File-name convention on MD site (2004-2012):

    general election
        precinct:            countyname_by_precinct_year_general.csv
        state leg. district: state_leg_districts_year_general.csv
        county:              countyname_party_year_general.csv

    primary election
        precinct:            countyname_by_Precinct_party_year_Primary.csv
        state leg. district: state_leg_districts_party_year_primary.csv
        county:              countyname_party_year_primary.csv

    Exceptions: 2000 + 2002

"""
import re

from openelex.api import elections as elec_api
from openelex.base.fetch import BaseFetcher


class FetchResults(BaseFetcher):

    base_url = "http://www.elections.state.md.us/elections/%(year)s/election_data/"

    def run(self, year):
        elections = elec_api.find(self.state, year)
        meta = self.build_metadata(year, elections)
        # DOWNLOAD FILES
        for item in meta:
            self.fetch(item['raw_url'], item['generated_filename'])

        #TODO: re-enable mapping updates
        # UPDATE md/filenames.json
        #self.update_mappings(year, meta)

    def build_metadata(self, year, elections):
        year_int = int(year)
        if year_int == 2000:
            general, primary = self.races_by_type(elections)
            meta = [
                {
                    "generated_name": "__".join((primary['start_date'].replace('-',''), self.state, "primary.csv")),
                    "raw_url": 'http://www.elections.state.md.us/elections/2000/results/prepaa.csv',
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": primary['slug']
                }
            ]
        elif year_int == 2002:
            general, primary = self.races_by_type(elections)
            meta = [
                {
                    "generated_name": "__".join((general['start_date'].replace('-',''), self.state, "general.txt")),
                    "raw_url": 'http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt',
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": general['slug']
                },
                {
                    "generated_name": "__".join((primary['start_date'].replace('-',''), self.state, "primary.txt")),
                    "raw_url": 'http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt',
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": primary['slug']
                }
            ]
        else:
            meta = self.state_leg_meta(year, elections) + self.county_meta(year, elections)
        return meta

    def state_leg_meta(self, year, elections):
        payload = []
        meta = {
            'ocd_id': 'ocd-division/country:us/state:md/sldl:all',
            'name': 'State Legislative Districts',
        }

        general, primary = self.races_by_type(elections)

        # Add General meta to payload
        general_url = self.build_state_leg_url(year)
        general_filename = self.generate_state_leg_filename(general_url, general['start_date'])
        gen_meta = meta.copy()
        gen_meta.update({
            'raw_url': general_url,
            'generated_filename': general_filename,
            'election': general['slug']
        })
        payload.append(gen_meta)

        # Add Primary meta to payload
        if primary and int(year) > 2000:
            for party in ['Democratic', 'Republican']:
                pri_meta = meta.copy()
                primary_url = self.build_state_leg_url(year, party)
                primary_filename = self.generate_state_leg_filename(primary_url, primary['start_date'])
                pri_meta.update({
                    'raw_url': primary_url,
                    'generated_filename': primary_filename,
                    'election': primary['slug']
                })
                payload.append(pri_meta)
        return payload

    def races_by_type(self, elections):
        "Filter races by type and add election slug"
        general = filter(lambda elec: elec['race_type'] == 'general', elections)[0]
        general['slug'] = "-".join((self.state, general['start_date'], 'general'))
        primary = filter(lambda elec: elec['race_type'] == 'primary', elections)[0]
        primary['slug'] = "-".join((self.state, primary['start_date'], 'primary'))
        return general, primary

    def build_state_leg_url(self, year, party=""):
        tmplt = self.base_url + "State_Legislative_Districts"
        kwargs = {'year': year}
        year_int = int(year)
        # PRIMARY
        # Assume it's a primary if party is present
        if party and year_int > 2000:
            kwargs['party'] = party
            if year_int == 2004:
                tmplt += "_%(party)s_Primary_%(year)s"
            else:
                tmplt += "_%(party)s_%(year)s_Primary"
        # GENERAL
        else:
            # 2000 and 2004 urls end in the 4-digit year
            if year_int in (2000, 2004):
                tmplt += "_General_%(year)s"
            # All others have the year preceding the race type (General/Primary)
            else:
                tmplt += "_%(year)s_General"
        tmplt += ".csv"
        return tmplt % kwargs

    def county_meta(self, year, elections):
        payload = []
        general, primary = self.races_by_type(elections)

        for jurisdiction in self.jurisdictions():

            meta = {
                'ocd_id': jurisdiction['ocd_id'],
                'name': jurisdiction['name'],
            }

            county = jurisdiction['url_name']

            # GENERALS
            # Create countywide and precinct-level metadata for general
            for precinct_val in (True, False):
                general_url = self.build_county_url(year, county, precinct=precinct_val)
                general_filename = self.generate_county_filename(general_url, general['start_date'], jurisdiction)

            # Add General metadata to payload
            gen_meta = meta.copy()
            gen_meta.update({
                'raw_url': general_url,
                'generated_filename': general_filename,
                'election': general['slug']
            })
            payload.append(gen_meta)

            # PRIMARIES
            # For each primary and party and party combo, generate countywide and precinct metadata
            if primary and int(year) > 2000:
                for party in ['Democratic', 'Republican']:
                    for precinct_val in (True, False):
                        pri_meta = meta.copy()
                        primary_url = self.build_county_url(year, county, party, precinct_val)
                        primary_filename = self.generate_county_filename(primary_url, year, jurisdiction)
                        # Add Primary metadata to payload
                        pri_meta.update({
                            'raw_url': primary_url,
                            'generated_filename': primary_filename,
                            'election': primary['slug']
                        })
                        payload.append(pri_meta)

        return payload

    def build_county_url(self, year, name, party='', precinct=False):
        url_kwargs = {
            'year': year,
            'race_type': 'General'
        }
        tmplt = self.base_url + name
        if precinct:
            tmplt += "_By_Precinct"
        else:
            tmplt += "_County"
        if party:
            url_kwargs['party'] = party
            url_kwargs['race_type'] = 'Primary'
            tmplt += "_%(party)s"
        if int(year) == 2004:
            tmplt += "_%(race_type)s_%(year)s.csv"
        else:
            tmplt += "_%(year)s_%(race_type)s.csv"
        return tmplt % url_kwargs

    def get_2002_source_url(self, race_type):
        if race_type == 'general':
            url = "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt"
        elif race_type == 'primary':
            url = "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"
        else:
            url = None
        return url

    def jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['url_name'] != ""]
        return mappings

    def generate_state_leg_filename(self, url, start_date):
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
        ]
        matches = self._apply_party_racetype_regex(url)
        if matches['party']:
            bits.append(matches['party'].lower())
        bits.extend([
            matches['race_type'].lower(),
            'state_legislative.csv',
        ])
        name = "__".join(bits)
        return name

    def generate_county_filename(self, url, start_date, jurisdiction):
        bits = [
            start_date.replace('-',''),
            self.state,
        ]
        matches = self._apply_party_racetype_regex(url)
        if matches['party']:
            bits.append(matches['party'].lower())
        bits.extend([
            matches['race_type'].lower(),
            jurisdiction['url_name'].lower()
        ])
        if 'by_precinct' in url.lower():
            bits.append('precinct')
        filename = "__".join(bits) + '.csv'
        return filename

    def _apply_party_racetype_regex(self, url):
        if re.search(r'(2000|2004)', url):
            pattern = re.compile(r"""
                (?P<party>Democratic|Republican)?
                _
                (?P<race_type>General|Primary)""", re.IGNORECASE | re.VERBOSE)
        else:
            pattern = re.compile(r"""
                (?P<party>Democratic|Republican)?
                _\d{4}_
                (?P<race_type>General|Primary)""", re.IGNORECASE | re.VERBOSE)
        matches = re.search(pattern, url).groupdict()
        return matches

    def generate_2002_filename(self, url):
        if url.endswith('g_all_offices.txt'):
            filename = "20021105__md__general.txt"
        else:
            filename = "20020910__md__primary.txt"
        return filename
