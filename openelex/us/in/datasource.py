"""Provides mappings between various election metadata and datasources.

Indiana has CSV files containing county-level and some precinct-level
results back to 2002.  The files are pre-processed and available on
Github at https://github.com/openelections/openelections-data-in.

Most results are stored as a single file per election.  Some
precinct-level results have each county stored in a separate results
file.
"""

from openelex.base.datasource import BaseDatasource
from openelex.lib import build_raw_github_url


class Datasource(BaseDatasource):

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return list of dicts linking urls, filenames, and election ids."""
        mappings = []
        for yr, elecs in list(self.elections(year).items()):
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        """Returns list of source data urls, optionally filtered by year."""
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        """Returns list of (filename, url) pairs, optional filtered by year."""
        return [(mapping['generated_filename'], mapping['pre_processed_url'])
                for mapping in self.mappings(year)]

    # PRIVATE METHODS
    def _build_metadata(self, year, elections):
        meta = []
        for election in elections:
            results = [x for x in self._url_paths()
                       if x['date'] == election['start_date'] and
                       not election['special']]
            for result in results:
                meta.append({
                    "generated_filename": self._standardized_filename(election),
                    "raw_url": result['url'],
                    "pre_processed_url": build_raw_github_url(
                        self.state, year, result['path']),
                    "ocd_id": 'ocd-division/country:us/state:in',
                    "name": 'Indiana',
                    "election": election['slug']
                })
        return meta
