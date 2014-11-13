
from openelex.api import elections as elec_api
from openelex.base.datasource import BaseDatasource

"""

Illinois' election results portal is
http://www.elections.il.gov/ElectionInformation/DownloadVoteTotals.aspx

This page offers results at the county level and contain all races
in a single file.

Special election results can be retrieved starting at
http://www.elections.il.gov/ElectionInformation/GetVoteTotals.aspx.

These are available on a per-contest basis.

County results are available, but uses ASP.NET's postback feature to toggle
the county results.
http://www.evagoras.com/2011/02/10/how-postback-works-in-asp-net/
provides a good explanation of how this feature works.

The ``direct_links`` fields from the API appear to point to the correct
starting point for the postback.

"""

class Datasource(BaseDatasource):
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

    def _ocd_id(self, election):
        return "ocd-division/country:us/state:il"

    def _build_metadata(self, year, elections):
        meta = []
        wrong_num_links_msg = ("There should be 1 direct link for election "
            "{}, instead found {}")
        for election in elections:
            # There should be one and only one direct link for all elections.
            num_direct_links = len(election['direct_links'])
            assert num_direct_links == 1, wrong_num_links_msg.format(
                election['slug'], num_direct_links)

            meta.append({
                "generated_filename": self._generate_filename(election),
                "raw_url": election['direct_links'][0],
                "ocd_id": self._ocd_id(election),
                "name": self.state,
                "election": election['slug']
            })
        return meta

    def _generate_filename(self, election):
        extension = self._filename_extension_for_election(election)
        if election['race_type'] == 'primary-runoff':
            race_type = 'primary_runoff'
        else:
            race_type = election['race_type']

        if election['special']:
            race_type = '__special' + race_type

        bits = [
            election['start_date'].replace('-',''),
            self.state.lower(),
            race_type,
        ]

        if not election['special']:
            # Non-special elections provide results at a county level.
            # Special election results are also available at a county level,
            # but you have to jump through some ASP.NET hoops to get to them.
            # See https://github.com/openelections/core/issues/77.
            # The value in the direct_links field of the election record
            # currently points to the racewide results, so just use that
            # for now.
            bits.append('county')

        name = "__".join(bits) + extension

        return name

    def _filename_extension_for_election(self, election):
        if election['special']:
            return ".html"
        else:
            return ".csv"
