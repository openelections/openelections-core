"""
Standardize names of data files on Florida Department of State.

File-name conventions on FL site are very consistent: tab-delimited text files containing county-level results are retrieved by election date:

    https://doe.dos.state.fl.us/elections/resultsarchive/ResultsExtract.Asp?ElectionDate=1/26/2010&OfficialResults=N&DataMode=

These are represented in the dashboard API as the `direct_links` attribute on elections.
"""

from openelex.api import elections as elec_api
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

    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            # Store elections by year
            self._elections = {}
            for elec in elec_api.find(self.state):
                rtype = elec['race_type'].lower()
                elec['slug'] = "-".join((self.state, elec['start_date'], rtype))
                yr = int(elec['start_date'][:4])
                self._elections.setdefault(yr, []).append(elec)
        if year:
            year_int = int(year)
            return {year_int: self._elections[year_int]}
        return self._elections

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        for election in elections:
            meta.append({
                "generated_filename": self._generate_filename(election),
                "raw_url": election['direct_links'][0],
                "ocd_id": 'ocd-division/country:us/state:fl',
                "name": 'Florida',
                "election": election['slug']
            })
        return meta
    
    def _generate_filename(self, election):
        # example: 20021105__fl__general.tsv
        if election['race_type'] == 'primary-runoff':
            race_type = 'primary__runoff'
        else:
            race_type = election['race_type']
        if election['special'] == True:
            race_type = 'special__' + race_type
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            race_type
        ]
        name = "__".join(bits) + '.tsv'
        return name
    
    def _jurisdictions(self):
        """Florida counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m]
        return mappings
