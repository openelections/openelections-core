"""
Retrieves CSV result files for a given year from Maryland State Board of Elections and caches them locally.

File name for general election precinct-level files is county_name_by_precinct_year_general.csv
File name for general election state legislative district-level files is State_Legislative_Districts_year_general.csv
File name for general election county-level files is County_Name_party_year_general.csv

File name for primary election precinct-level files is County_Name_by_Precinct_party_year_Primary.csv
File name for primary election state legislative district-level files is State_Legislative_Districts_party_year_primary.csv
File name for primary election county-level files is County_Name_party_year_primary.csv

Exceptions: 2004 and 2000 files put the year at the end of the file name, and 2002 has single text files with all results for a single election. 2000 primary is also a single csv file.

Usage:

from openelex.us.md import fetch
f = fetch.FetchResults()
f.run(2012)
"""

from openelex.base.fetch import BaseFetcher

class FetchResults(BaseFetcher):

    def run(self, year, urls=None):
        filenames = {}
        elections = self.api_response(self.state, year)

        if year == 2002:
            # generate 2002 files, which have single text files for all results
            general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
            primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]

            g_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general.txt"
            p_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__primary.txt"

            urls = [
                [
                    g_generated_name,
                    'http://www.elections.state.md.us/elections/2002/results/g_all_offices.txt',
                    'ocd-division/country:us/state:md',
                    'Maryland',
                    general['id']
                ],
                [
                    p_generated_name,
                    'http://www.elections.state.md.us/elections/2002/results/p_all_offices.txt',
                    'ocd-division/country:us/state:md',
                    'Maryland',
                    primary['id']
                ]
            ]

        elif year == 2000:
            general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
            primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]

            g_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general.txt"
            p_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__primary.csv"

            urls = self.state_legislative_district_urls(year, elections) + self.county_urls(year, elections)
            urls.append([
                p_generated_name,
                'http://www.elections.state.md.us/elections/2000/results/prepaa.csv',
                'ocd-division/country:us/state:md',
                'Maryland',
                primary['id']
            ])
        else:
            if not urls:
                urls = self.state_legislative_district_urls(year, elections) + self.county_urls(year, elections)

        # DOWNLOAD FILES
        for generated_name, raw_url, ocd_id, name, election in urls:
            self.fetch(raw_url, generated_name)

        # UPDATE FILENAMES.JSON
        filenames = [
            {
                'generated_name': generated_name,
                'ocd_id': ocd_id,
                'raw_url': raw_url,
                'name': name,
                'election': election
            } for generated_name, raw_url, ocd_id, name, election in urls
        ]
        self.update_mappings(year, filenames)

    def state_legislative_district_urls(self, year, elections):
        urls = []
        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]

        generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__state_legislative.csv"
        if year in (2000, 2004):
            raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_General_%s.csv" % (year, year)
        else:
            raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_General.csv" % (year, year)
        urls.append([
            generated_name,
            raw_name,
            'ocd-division/country:us/state:md/sldl:all',
            'State Legislative Districts',
            general['id']
        ])

        if primary and year > 2000:
            for party in ['Democratic', 'Republican']:
                generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__state_legislative.csv"
                if year == 2004:
                    raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_Primary_%s.csv" % (year, party, year)
                else:
                    raw_name = "http://www.elections.state.md.us/elections/%s/election_data/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year)
                urls.append([
                    generated_name,
                    raw_name,
                    'ocd-division/country:us/state:md/sldl:all',
                    'State Legislative Districts',
                primary['id']])
        return urls

    def county_urls(self, year, elections):
        urls = []

        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]

        for jurisdiction in self.jurisdictions():
            county_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s.csv" % jurisdiction['url_name'].lower()
            if year in (2000, 2004):
                county_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_General_%s.csv" % (year, jurisdiction['url_name'], year)
            else:
                county_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([
                county_generated_name,
                county_raw_name,
                jurisdiction['ocd_id'],
                jurisdiction['name'],
                general['id']
            ])
            precinct_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s__precinct.csv" % jurisdiction['url_name'].lower()
            if year in (2000, 2004):
                precinct_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_General_%s.csv" % (year, jurisdiction['url_name'], year)
            else:
                precinct_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([
                precinct_generated_name,
                precinct_raw_name,
                jurisdiction['ocd_id'],
                jurisdiction['name'],
                general['id']
            ])


            if primary and year > 2000:
                for party in ['Democratic', 'Republican']:
                    county_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__%s.csv" % jurisdiction['url_name'].lower()
                    if year == 2004:
                        county_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_Primary_%s.csv" % (year, jurisdiction['url_name'], party, year)
                    else:
                        county_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_%s_Primary.csv" % (year, jurisdiction['url_name'], party, year)

                    urls.append([
                        county_party_generated_name,
                        county_party_raw_name,
                        jurisdiction['ocd_id'],
                        jurisdiction['name'],
                        primary['id']
                    ])
                    precinct_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__%s__precinct.csv" % jurisdiction['url_name'].lower()
                    if year == 2004:
                        precinct_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_Primary_%s.csv" % (year, jurisdiction['url_name'], party, year)
                    else:
                        precinct_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_%s_Primary.csv" % (year, jurisdiction['url_name'], party, year)

                    urls.append([precinct_party_generated_name, precinct_party_raw_name, jurisdiction['ocd_id'], jurisdiction['name'], primary['id']])
        return urls

    def jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings(('ocd_id','fips','url_name', 'name'))
        mappings = [x for x in m if x['url_name'] != ""]
        return mappings
