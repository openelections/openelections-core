
from openelex.api import elections as elec_api
from openelex.base.datasource import BaseDatasource

"""

Illinois' election results portal is
http://www.elections.il.gov/ElectionInformation/DownloadVoteTotals.aspx 

They appear to be aggregated to the county level and contain all races
in a single file.

"""

class Datasource(BaseDatasource):
    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            # Store elections by year
            self._elections = {}
            for elec in elec_api.find(self.state):
                yr = int(elec['start_date'][:4])
                # Add elec slug
                elec['slug'] = self._elec_slug(elec)
                self._elections.setdefault(yr, []).append(elec)
        if year:
            year_int = int(year)
            return {year_int: self._elections[year_int]}
        return self._elections

    def mappings(self, year=None):
        """Return array of dicts  containing source url and 
        standardized filename for raw results file, along 
        with other pieces of metadata

        Metadata dictionaries should contain the following items:

        * election
        * generated_filename
        * name
        * ocd_id
        * raw_url

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

    def _elec_slug(self, election):
        bits = [
            self.state,
            election['start_date'],
            election['race_type'].lower()
        ]
        return "-".join(bits)

    def _ocd_id(self, election):
        base = "ocd-division/country:us/state:il" 
        return base

    def _get_raw_url(election):
        direct_link = election.get('direct_link')
        if direct_link: 
            return direct_link 

        # Dire


    def _build_metadata(self, year, elections):
        meta = []
        for election in elections:
            # It seems like there should be direct links for all elections.
            # If there isn't what should we do?  For instance there isn't
            # a direct link for the 2012 general election with 
            # slug il-2012-11-06-general. Is this a data entry error on the
            # dashboard?
            if election.get('direct_link') == '':
                # QUESTION: Best way to let warnings trickle up?
                print "No direct link for election %s" % (election['slug'])

            meta.append({
                "generated_filename": self._generate_filename(election),
                "raw_url": election['direct_link'],
                "ocd_id": self._ocd_id(election), 
                "name": self.state, 
                "election": election['slug']
            })
        return meta

    def _generate_filename(self, election):
        # example: 20021105__fl__general.txt
        if election['race_type'] == 'primary-runoff':
            race_type = 'primary__runoff'
        else:
            race_type = election['race_type']

        if election['special'] == True:
            race_type = race_type + '__special'

        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            race_type
        ]

        # QUESTION: What's the hierarchy of these levels? For instance, an
        # election record from the API for Illinois has both county_level and
        # state_level set to true.  Currently, I'm basing the hierarchy based
        # on the order the results are listed at
        # http://docs.openelections.net/election-metadata/ 
        result_levels = ['state_leg', 'cong_dist', 'precinct',
            'county',]
        for level in result_levels:
            if election[level + '_level']:
                bits.append(level)
                break

        name = "__".join(bits) + '.txt'

        return name
