"""
Standardize names of data files on Idaho Department of State.

File-name conventions on ID site are fairly consistent: Excel files containing county-level and precinct-level results:

    http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_fed_prec.xls
    http://www.sos.idaho.gov/elect/RESULTS/2012/General/12%20Gen_leg_prec.xls
    
although earlier files have a slightly different format:

    http://www.sos.idaho.gov/elect/2000rslt/primary/00_pri_fed.xls
    http://www.sos.idaho.gov/elect/2000rslt/general/00_gen_lgpc.xls

There are two files for each election; one covers federal and statewide offices, with the other covers the state legislature. We put these into a dict
structured like so:
    
    {'primary_statewide':'http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_fed_prec.xls', 'primary_state_legislature':'http://www.sos.idaho.gov/elect/RESULTS/2012/Primary/12%20Pri_leg_prec.xls'}

"""
import requests
from bs4 import BeautifulSoup

from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):
    
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

    def results_links(self, year):
        if not hasattr(self, '_results_links'):
            url = "http://www.sos.idaho.gov/elect/results.htm"
            r = requests.get(url)
            soup = BeautifulSoup(r.text)
            table = soup.find_all('table')[3]
            precincts = [x for x in table.find_all('a') if str(year) in x['href']]
            self._results_links = [x for x in precincts if x.text == 'Statewide' or 'Legislature' in x.text]
        return self._results_links
        
    # PRIVATE METHODS
    
    def __filter_results_links(self, year):
        links = {}
        for link in [x for x in self.results_links(year)]:
            if link.text == 'Statewide' and 'rimary' in link['href'] and ('prec' in link['href'] or 'pct' in link['href'] or 'pri_fed' in link['href'] or 'pri_lgpc' in link['href']):
                if year == 2004:
                    links['primary_statewide'] = 'http://www.sos.idaho.gov'+link['href']
                elif year < 2004:
                    links['primary_statewide'] = link['href']
                else:
                    links['primary_statewide'] = 'http://www.sos.idaho.gov/elect/'+link['href']
            elif 'Legislature' in link.text and 'rimary' in link['href'] and ('prec' in link['href'] or 'pct' in link['href'] or 'pri_fed' in link['href'] or 'pri_lgpc' in link['href']):
                if year == 2004:
                    links['primary_state_legislature'] = 'http://www.sos.idaho.gov/'+link['href']
                elif year < 2004:
                    links['primary_state_legislature'] = link['href']
                else:
                    links['primary_state_legislature'] = 'http://www.sos.idaho.gov/elect/'+link['href']
            elif link.text == 'Statewide' and ('prec' in link['href'] or 'pct' in link['href'] or 'en_fed' in link['href']):
                if year == 2004:
                    links['general_statewide'] = 'http://www.sos.idaho.gov'+link['href']
                elif year < 2004:
                    links['general_statewide'] = link['href']
                else:
                    links['general_statewide'] = 'http://www.sos.idaho.gov/elect/'+link['href']
            elif 'Legislature' in link.text and ('prec' in link['href'] or 'pct' in link['href'] or 'pri_fed' in link['href'] or 'gen_lgpc' in link['href'] or 'Gen_leg' in link['href']):
                if year == 2004:
                    links['general_state_legislature'] = 'http://www.sos.idaho.gov'+link['href']
                elif year < 2004:
                    links['general_state_legislature'] = link['href']
                else:
                    links['general_state_legislature'] = 'http://www.sos.idaho.gov/elect/'+link['href']
        try:
            links['primary_state_legislature']
        except:
            print links
        return [links['primary_statewide'], links['primary_state_legislature']], [links['general_statewide'], links['general_state_legislature']]
    
    def __build_absentee_metadata(self, year, election):
        # absentee files available from 2002-forward
        if election['race_type'] == 'general':
            slug = str(year)[2:4] + 'Gen'
            title = 'general'
        else:
            slug = str(year)[2:4] + 'Pri'
            title = 'primary'
        raw_url = "http://www.sos.idaho.gov/elect/absentee/%s_Absentee.xls" % slug
        return {
                    "generated_filename": self._generate_filename(election, absentee=True, title=title),
                    "raw_url": raw_url,
                    "ocd_id": 'ocd-division/country:us/state:id',
                    "name": 'Idaho',
                    "election": election['slug']
                }

    def _build_metadata(self, year, elections):
        # TODO: need to handle multiple files per election
        # ID has two files for each election, grab them from results_links using
        # following conventions: [primary|general]_[year]_[statewide\state_legislature].xls
        meta = []
        year_int = int(year)
        primary, general = self.__filter_results_links(year)
        for election in elections:
            if election['race_type'] == 'primary':
                statewide, state_legislative = primary
            else:
                statewide, state_legislative = general
            if year > 2000:
                meta.append(self.__build_absentee_metadata(year, election))
            meta.append({
                "generated_filename": self._generate_filename(election, absentee=False, title='statewide'),
                "raw_url": statewide,
                "ocd_id": 'ocd-division/country:us/state:id',
                "name": 'Idaho',
                "election": election['slug']
            })
            meta.append({
                "generated_filename": self._generate_filename(election, absentee=False, title='state_legislative'),
                "raw_url": state_legislative,
                "ocd_id": 'ocd-division/country:us/state:id',
                "name": 'Idaho',
                "election": election['slug']
            })
        return meta
    
    def _generate_filename(self, election, absentee=False, title=None):
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            title
        ]
        if absentee:
            bits.append('absentee')
        name = "__".join(bits) + '.xls'
        return name
    
    def _jurisdictions(self):
        """Idaho counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m]
        return mappings
