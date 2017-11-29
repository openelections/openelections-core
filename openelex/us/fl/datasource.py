"""
Standardize names of data files on Florida Department of State.

File-name conventions on FL site are very consistent: tab-delimited text files containing county-level results are retrieved by election date:

    https://doe.dos.state.fl.us/elections/resultsarchive/ResultsExtract.Asp?ElectionDate=1/26/2010&OfficialResults=N&DataMode=

These are represented in the dashboard API as the `direct_links` attribute on elections.
"""

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
        meta = []
        for election in elections:
            meta.append({
                "generated_filename": self._generate_filename(election),
                "raw_url": election['direct_links'][0],
                "ocd_id": 'ocd-division/country:us/state:fl',
                "name": 'Florida',
                "election": election['slug']
            })
            if year > 2011:
                meta.append({
                    "generated_filename": self._generate_precinct_filename(election),
                    "raw_url": election['direct_links'][1],
                    "ocd_id": 'ocd-division/country:us/state:fl',
                    "name": 'Florida',
                    "election": election['slug']
                })

        return meta

    def _generate_filename(self, election):
        # example: 20021105__fl__general.tsv
        if election['race_type'] == 'primary-runoff':
            race_type = 'primary_runoff'
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

    def _generate_precinct_filename(self, election):
        if election['special']:
            election_type = 'special__' + election['race_type'].replace("-","__") + '__precinct'
        else:
            election_type = election['race_type'].replace("-","__") + '__precinct'
        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            election_type
        ]
        name = "__".join(bits) + '.tsv'
        return name

    def _jurisdictions(self):
        """Florida counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m]
        return mappings
