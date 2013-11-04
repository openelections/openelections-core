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
from datetime import datetime
from openelex.base.fetch import BaseFetcher

class MDFetcher(BaseFetcher):

    def __init__(self):
        #TODO: for year in years: get source urls
        #ocd_id = 'ocd-division/country:us/state:md/sldl:all'
        self.base_url = "http://www.elections.state.md.us/elections/%(year)s/election_data/"

    #TODO: Construct target url
    #TODO: Standardize name


    @property
    def target_urls(self):
        urls = []

        # For each year, get urls for primaries and generals
        for year in self.target_years:
            for race_type in ('Primary', 'General'):
                if race_type == 'Primary':
                    for party in ['Democratic', 'Republican']:
                        urls.append(self.state_leg_url(year, race_type, party))
                else:
                    urls.append(self.state_leg_url(year, race_type))
        return urls

    def state_leg_url(self, year, race_type, party=''):
        #TODO: URL examples: "http://www.elections.state.md.us/elections/2009/election_data/State_Legislative_Districts_2009_Democratic_Primary.csv"
        url_tmplt = self.base_url + "State_Legislative_Districts_%(year)s_"
        general['start_date']

        kwargs = {
            'year': year,
            'race_type': race_type,
            'party': party
        }
        # If a party is present, assume it's a primary. this is naive
        if kwargs['party']:
            url_primary = url_tmplt + "%(race_type)s.csv"
            url = url_primary % kwargs
        else:
            url_gen = url_tmplt + "%(party)s_%(race_type)s.csv"
            url = url_gen % kwargs
        return url

    def standardize_name(self, url, race_type, start_date):
        start_date = start_date.replace('-','')
        if 'state_leg' in url.lower():
            bits = (
                start_date,
                "__",
                self.state,
                "__general__state_legislative.csv"
            )
            name = "".join(bits)
        elif 'county' in url.lower():
            pass
            name = ""
        return name

        """
        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]

        urls.append([
            generated_name,
            raw_name,
            ocd_id,
            'State Legislative Districts',
            general['id']
        ])

        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]

        raw_name = self.base_url + "/State_Legislative_Districts_%s_%s_Primary.csv" % (year, party, year)
        for party in ['Democratic', 'Republican']:
            generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__state_legislative.csv"
            urls.append([
                generated_name,
                raw_name,
                'ocd-division/country:us/state:md/sldl:all',
                'State Legislative Districts',
                primary['id']
            ])
        """
        return urls

    def county_urls(self):
        urls = []
        general = [e for e in elections['elections'] if e['election_type'] == 'general'][0]
        primary = [e for e in elections['elections'] if e['election_type'] == 'primary'][0]
        for jurisdiction in self.jurisdictions():
            county_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s.csv" % jurisdiction['url_name'].lower()
            county_raw_name = "%s_County_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([county_generated_name, county_raw_name, jurisdiction['ocd_id'], jurisdiction['name'], general['id']])
            precinct_generated_name = general['start_date'].replace('-','')+"__"+self.state+"__general__%s__precinct.csv" % jurisdiction['url_name'].lower()
            precinct_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_General.csv" % (year, jurisdiction['url_name'], year)
            urls.append([precinct_generated_name, precinct_raw_name, jurisdiction['ocd_id'], jurisdiction['name'], general['id']])
            for party in ['Democratic', 'Republican']:
                county_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__%s.csv" % jurisdiction['url_name'].lower()
                county_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_County_%s_%s_Primary.csv" % (year, jurisdiction['url_name'], party, year)
                urls.append([county_party_generated_name, county_party_raw_name, jurisdiction['ocd_id'], jurisdiction['name'], primary['id']])
                precinct_party_generated_name = primary['start_date'].replace('-','')+"__"+self.state+"__"+party.lower()+"__primary__%s__precinct.csv" % jurisdiction['url_name'].lower()
                precinct_party_raw_name = "http://www.elections.state.md.us/elections/%s/election_data/%s_By_Precinct_%s_%s_Primary.csv" % (year, jurisdiction['url_name'], party, year)
                urls.append([precinct_party_generated_name, precinct_party_raw_name, jurisdiction['ocd_id'], jurisdiction['name'], primary['id']])
        return urls

    def jurisdictions(self):
        """Maryland counties, plus Baltimore City"""
        m = self.jurisdiction_mappings(('ocd_id','fips','url_name', 'name'))
        return m

if __name__ == '__main__':
    from pprint import pprint as pp
    md =MDFetcher()
    #print md.base_url
    pp(md.target_urls)
