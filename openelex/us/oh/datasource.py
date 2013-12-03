"""
Standardize names of data files on Ohio Secretary of State and 
save to mappings/filenames.json

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
import os
from os.path import join
import re
import json
import unicodecsv
import urlparse
import scrapelib
from bs4 import BeautifulSoup

from openelex.api import elections as elec_api
from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):
    
    base_url = "http://www.sos.state.oh.us/sos/elections/Research/electResultsMain/%(year)sElectionsResults/"
    
    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts containing source url and 
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

    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            # Store elections by year
            self._elections = {}
            for elec in elec_api.find(self.state):
                yr = int(elec['start_date'][:4])
                self._elections.setdefault(yr, []).append(elec)
        if year:
            year_int = int(year)
            return {year_int: self._elections[year_int]}
        return self._elections

    # PRIVATE METHODS
    def _races_by_type(self, elections):
        "Filter races by type and add election slug"
        races = {} 
        for elec in elections:
            rtype = elec['race_type'].lower()
            elec['slug'] = "-".join((self.state, elec['start_date'], rtype))
            races[rtype] = elec
        return races['general'], races['primary']

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        # if precinct-level files available, just grab those - general and primary only
        precinct_elections = [e for e in elections if e['precinct_level'] == True]
        other_elections = [e for e in elections if e['precinct_level'] == False]
        if precinct_elections:
            meta.append(self._precinct_meta(year, elections))
        if other_elections:
            for election in other_elections:
                if election['portal_link'] == election['direct_link']:
                    results_links = self._results_links(self, year, election['portal_link'])
                    # generate offices to filter for
                    # filter offices
                    for link in results_links:
                        meta.append(self._)
                        meta.append({
                            "generated_filename": self._generate_office_filename(election['direct_link'], election['start_date'], election['race_type'], office),
                            "raw_url": election['direct_link'],
                            "ocd_id": 'ocd-division/country:us/state:oh',
                            "name": 'Ohio',
                            "election": election['slug']
                        })
                else:
                    # always a special election
                    meta.append({
                        "generated_filename": self.__generate_special_filename(election['direct_link'], election['start_date'], election['race_type']),
                        "raw_url": election['direct_link'],
                        "ocd_id": 'ocd-division/country:us/state:oh',
                        "name": 'Ohio',
                        "election": election['slug']
                    })
                    # special case for 20050614
                    if election['direct_link'] == 'https://www.sos.state.oh.us/sos/elections/Research/electResultsMain/2005ElectionsResults/05-0614Dem2ndCongDist.aspx':
                        meta.append({
                            "generated_filename": self.__generate_special_filename('https://www.sos.state.oh.us/sos/elections/Research/electResultsMain/2005ElectionsResults/05-0614Rep2ndCongDist.aspx', election['start_date'], election['race_type']),
                            "raw_url": 'https://www.sos.state.oh.us/sos/elections/Research/electResultsMain/2005ElectionsResults/05-0614Rep2ndCongDist.aspx',
                            "ocd_id": 'ocd-division/country:us/state:oh',
                            "name": 'Ohio',
                            "election": election['slug']
                        })
        return meta

    def _precinct_meta(self, year, elections):
        payload = []
        meta = {
            'ocd_id': 'ocd-division/country:us/state:oh/precinct:all',
            'name': 'Precincts',
        }

        general, primary = self._races_by_type(elections)

        # Add General meta to payload
        general_url = general['source_url']
        general_filename = self._generate_precinct_filename(general_url, general['start_date'], 'general')
        gen_meta = meta.copy()
        gen_meta.update({
            'raw_url': general_url,
            'generated_filename': general_filename,
            'election': general['slug']
        })
        payload.append(gen_meta)

        # Add Primary meta to payload
        if primary and int(year) > 2000:
            pri_meta = meta.copy()
            primary_url = primary['source_url']
            primary_filename = self._generate_precinct_filename(primary_url, primary['start_date'], 'primary')
            pri_meta.update({
                'raw_url': primary_url,
                'generated_filename': primary_filename,
                'election': primary['slug']
            })
            payload.append(pri_meta)
        return payload

    def _build_state_leg_url(self, year, offices, party=""):
            tmplt = self.base_url
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

    def _generate_precinct_filename(self, url, start_date, election_type):
        # example: 20121106__oh__general__precincts.xlsx
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type
        ]
        path = urlparse.urlparse(url).path
        ext = os.path.splitext(path)[1]
        bits.extend(['precincts'])
        name = "__".join(bits)+ ext
        return name

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
                (?P<party>Dem|Rep|Lib)
                )""", re.IGNORECASE | re.VERBOSE)
        else:
            pattern = re.compile(r"""
                (?P<party>Democratic|Republican)?
                _\d{4}_
                (?P<race_type>General|Primary)""", re.IGNORECASE | re.VERBOSE)
        matches = re.search(pattern, url).groupdict()
        return matches

    def _generate_office_filename(self, url, start_date, election_type, office):
        # example: 20021105__oh__general__gov.aspx
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            election_type,
            office
        ]
        path = urlparse.urlparse(url).path
        ext = os.path.splitext(path)[1]
        name = "__".join(bits)+ ext
        return name

    def _generate_special_filename(self, url, start_date, election_type, party=None):
        # example: 20081118__oh__special__general.aspx
        bits = [
            start_date.replace('-',''),
            self.state.lower(),
            'special',
            election_type,
            party
        ]
        path = urlparse.urlparse(url).path
        ext = os.path.splitext(path)[1]
        name = "__".join(bits) + ext
        return name
   
    # returns a list of html links from results page
    def _results_links(self, year, election):
        s = scrapelib.Scraper(requests_per_minute=10, follow_robots=True)
        html = s.urlopen(election['portal_link'])
        soup = BeautifulSoup(html)
        links = [x.get('href') for x in soup.find_all('a')]
        results = [l for l in links if 'xlsx' in str(l)]
        # filter on paths for election
    
    def _url_paths(self):
        "Returns a JSON array of url path mappings"
        filename = join(self.mappings_dir, 'url_paths.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)
    
        
    def _url_date_segment(self, date):
        if date.year == 2000:
            fmt = date.strftime("%m%d%Y")
        elif date.year < 2008:
            fmt = date.strftime("%y-%m%d")
        elif date.year == 2008: # special only, not primary
            fmt = date.strftime("%m%d-%Y")
        elif date.year == 2010:
            fmt = date.strftime("%Y%m%d")
        return fmt
    
    def _jurisdictions(self):
        """Ohio counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['results_name'] != ""]
        return mappings
