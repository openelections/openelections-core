from builtins import next
from builtins import object
import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Wisconsin elections have pre-processed CSV results files for elections beginning in 2002.
These files contain ward-level results. The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-wi repository.
"""

class LoadResults(object):
      """
      Entry point for data loading.
      Determines appropriate loader for file and triggers load process.
      """

      def run(self, mapping):
          election_id = mapping['generated_filename']
          if 'precinct' in election_id:
              loader = ORPrecinctLoader()
          else:
              loader = ORLoader()
          loader.run(mapping)

class WIBaseLoader(BaseLoader):
      datasource = Datasource()

class WIPrecinctLoader(WIBaseLoader):
    """
    Loads Wisconsin precinct results for 2002-2017.

    Format:
    Wisconsin has Excel files that have been pre-processed to CSV for elections after 2002.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            next(reader, None)
            for row in reader:
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                rr_kwargs.update(self._build_jurisdiction_kwargs(row))
                rr_kwargs.update({
                    'primary_party': row['party'].strip(),
                    'party': row['party'].strip(),
                    'votes': int(float(row['votes']))
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _build_jurisdiction_kwargs(self, row):
            jurisdiction = row['ward'].strip()
            county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].strip().upper() == row['county'].strip().upper()][0]['ocd_id']       
            return {
                'jurisdiction': jurisdiction,
                'parent_jurisdiction': row['county'],
                'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(jurisdiction)),
            }

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip()
        }