import re

from openelex.api import elections as elec_api
from openelex.base.fetch import BaseFetcher
from .datasource import MDDatasource


class FetchResults(BaseFetcher):

    datasource = MDDatasource()

    def run(self, year):
        # DOWNLOAD FILES
        meta = self.datasource.mappings(year)
        for item in meta:
            self.fetch(item['raw_url'], item['generated_filename'])
