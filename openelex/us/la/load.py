import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Louisiana elections have pre-processed CSV results files for elections beginning in 2000. These files contain
county-level data for all of the state's parishes. Special election results are contained in election date-specific
files. The CSV versions of those are contained in the https://github.com/openelections/openelections-data-la repository.

Precinct-level data is coming.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['generated_filename']
        if 'precinct' in election_id:
            loader = LAPrecinctLoader()
        else:
            loader = LAParishLoader()
        loader.run(mapping)


class LABaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President',
        'U.S. Senate',
        'U.S. House',
        'Governor',
        'Lieutenant Governor',
        'Secretary of State',
        'Commissioner of Agriculture and Forestry',
        'Commissioner of Insurance',
        'State Treasurer',
        'Attorney General',
        'State Senate',
        'State House',
    ])

    district_offices = set([
        'U.S. House',
        'State Senate',
        'State House',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

class LAPrecinctLoader(LABaseLoader):
    """
    Loads Louisiana precinct results.

    Format: pre-processed CSV files
    """

    def load(self):
        headers = [
            'parish',
            'precinct',
            'office',
            'district',
            'party',
            'candidate',
            'votes'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs['primary_party'] = row['party'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = row['precinct'].strip()
                try:
                    county_ocd_id = [c for c in self.datasource._jurisdictions() if c['name'].upper().replace(' ','') == row['parish'].upper().replace(' ','')][0]['ocd_id']
                except:
                    print row
                    raise
                rr_kwargs.update({
                    'party': row['party'].strip(),
                    'jurisdiction': jurisdiction,
                    'parent_jurisdiction': row['parish'],
                    'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(row['precinct'])),
                    'office': row['office'].strip(),
                    'district': row['district'].strip(),
                    'votes': int(row['votes'].strip())
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['office'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip()
        }

class LAParishLoader(LABaseLoader):
    """
    Loads Louisiana parish-level results for 2000-2014 primary, general and special elections.

    Format: pre-processed CSVs.
    """

    def load(self):
        headers = [
            'parish',
            'office',
            'district',
            'party',
            'candidate',
            'votes'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'parish'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['office'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                ocd_type_id(row['parish'].strip())),
            'jurisdiction': row['parish'].strip(),
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip(),
            'votes': int(row['votes'].strip())
        }
