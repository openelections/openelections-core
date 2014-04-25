"""
Standardize names of data files on Maryland State Board of Elections.

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

To run mappings from invoke task:

    invoke datasource.mappings -s md

"""
import re

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
        if year_int == 2002:
            general, primary, special = self._races_by_type(elections)
            meta = [
                {
                    "generated_filename": "__".join((general['start_date'].replace('-',''), self.state, "general.txt")),
                    "raw_url": self._get_2002_source_urls('general'),
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": general['slug']
                },
                {
                    "generated_filename": "__".join((primary['start_date'].replace('-',''), self.state, "primary.txt")),
                    "raw_url": self._get_2002_source_urls('primary'),
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": primary['slug']
                }
            ]
        else:
            meta = self._state_leg_meta(year, elections) + self._county_meta(year, elections)
            if year_int == 2000:
                general, primary, special = self._races_by_type(elections)
                meta.append({
                    "generated_filename": "__".join((primary['start_date'].replace('-',''), self.state, "primary.csv")),
                    "raw_url": 'http://www.elections.state.md.us/elections/2000/results/prepaa.csv',
                    "ocd_id": 'ocd-division/country:us/state:md',
                    "name": 'Maryland',
                    "election": primary['slug']
                })
            elif year_int == 2008:
                meta.append(self._special_meta_2008(elections))

        return meta

    def _state_leg_meta(self, year, elections):
        payload = []
        meta = {
            'ocd_id': 'ocd-division/country:us/state:md/sldl:all',
            'name': 'State Legislative Districts',
        }

        general, primary, special = self._races_by_type(elections)

        # Add General meta to payload
        general_url = self._build_state_leg_url(year)
        general_filename = self._generate_state_leg_filename(general_url, general['start_date'])
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
                primary_url = self._build_state_leg_url(year, party)
                primary_filename = self._generate_state_leg_filename(primary_url, primary['start_date'])
                pri_meta.update({
                    'raw_url': primary_url,
                    'generated_filename': primary_filename,
                    'election': primary['slug']
                })
                payload.append(pri_meta)
        return payload

    def _build_state_leg_url(self, year, party=""):
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

    def _generate_state_leg_filename(self, url, start_date):
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

    def _county_meta(self, year, elections):
        payload = []
        general, primary, special = self._races_by_type(elections)

        for jurisdiction in self._jurisdictions():

            meta = {
                'ocd_id': jurisdiction['ocd_id'],
                'name': jurisdiction['name'],
            }

            county = jurisdiction['url_name']

            # GENERALS
            # Create countywide and precinct-level metadata for general
            for precinct_val in (True, False):
                general_url = self._build_county_url(year, county, precinct=precinct_val)
                general_filename = self._generate_county_filename(general_url, general['start_date'], jurisdiction)
                gen_meta = meta.copy()
                gen_meta.update({
                    'raw_url': general_url,
                    'generated_filename': general_filename,
                    'election': general['slug']
                })
                payload.append(gen_meta)

            # PRIMARIES
            # For each primary and party and party combo, generate countywide and precinct metadata
            # Primary results not available in 2000
            if primary and int(year) > 2000:
                for party in ['Democratic', 'Republican']:
                    for precinct_val in (True, False):
                        pri_meta = meta.copy()
                        primary_url = self._build_county_url(year, county, party, precinct_val)
                        primary_filename = self._generate_county_filename(primary_url, primary['start_date'], jurisdiction)
                        # Add Primary metadata to payload
                        pri_meta.update({
                            'raw_url': primary_url,
                            'generated_filename': primary_filename,
                            'election': primary['slug']
                        })
                        payload.append(pri_meta)

        return payload

    def _build_county_url(self, year, name, party='', precinct=False):
        url_kwargs = {
            'year': year,
            'race_type': 'General'
        }
        # In 2000, 2004 the files for St. Mary's county are prefixed
        # with "Saint_Marys" instead of "St._Marys".
        if name == "St._Marys" and int(year) in (2000, 2004):
            name = "Saint_Marys"
        tmplt = self.base_url + name
        if precinct:
            tmplt += "_By_Precinct"
        else:
            # 2000/2004 don't use "_County" in file names
            if int(year) not in (2000, 2004):
                tmplt += "_County"
        if party:
            url_kwargs['party'] = party
            url_kwargs['race_type'] = 'Primary'
            tmplt += "_%(party)s"
        if int(year) in (2000, 2004):
            tmplt += "_%(race_type)s_%(year)s.csv"
        else:
            tmplt += "_%(year)s_%(race_type)s.csv"
        return tmplt % url_kwargs

    def _generate_county_filename(self, url, start_date, jurisdiction):
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

    def _get_2002_source_urls(self, race_type=''):
        urls = {
            "general": "http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt",
            "primary": "http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt"
        }
        if race_type:
            return urls[race_type]
        else:
            return urls.values()

    def _generate_2002_filename(self, url):
        if url.endswith('g_all_offices.txt'):
            filename = "20021105__md__general.txt"
        else:
            filename = "20020910__md__primary.txt"
        return filename

    def _jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['url_name'] != ""]
        return mappings

    def _special_meta_2008(self, elections):
        """
        Return metadata for 2008 Special 4th Congressional General Election.
        """
        general, primary, special = self._races_by_type(elections)
        filename_bits = [
            special['start_date'].replace('-', ''),
            self.state,
            'special',
            'general',
            'us_house_of_representatives__4',
        ]
        return {
            "generated_filename": 
            "__".join(filename_bits) + ".html",
            "raw_url": special['direct_links'][0], 
            "ocd_id": 'ocd-division/country:us/state:md',
            "name": 'Maryland',
            "election": special['slug']
        }
