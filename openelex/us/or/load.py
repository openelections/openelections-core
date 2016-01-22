import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Oregon elections have pre-processed CSV results files for elections beginning in 2004. These files contain county-level data for each of the state's
counties; precinct-level data files are being added later. Special election results are contained in office-specific files. The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-or repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['generated_filename']
        if 'precinct' in election_id:
            loader = ORPrecinctLoader()
        else:
            loader = ORLoader()
        loader.run(mapping)


class ORBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President',
        'U.S. Senate',
        'U.S. House',
        'Governor',
        'Secretary of State',
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

class ORPrecinctLoader(ORBaseLoader):
    """
    Loads Oregon precinct results for 2004-2014.

    Format:

    Oregon has PDF files that have been converted to CSV files for elections after 2003.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            next(reader, None)
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['votes'] == 'X':
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs['primary_party'] = row['party'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = row['precinct'].strip()
                county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].strip().upper() == row['county'].strip().upper()][0]['ocd_id']
                rr_kwargs.update({
                    'party': row['party'].strip(),
                    'jurisdiction': jurisdiction,
                    'parent_jurisdiction': row['county'],
                    'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(jurisdiction)),
                    'office': row['office'].strip(),
                    'district': row['district'].strip(),
                    'votes': int(float(row['votes']))
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

class ORLoader(ORBaseLoader):
    """
    Loads Oregon county-level results for 2004-2014 elections.

    Format:

    Oregon has PDF files that have been converted to CSV files for elections after 2003.
    """

    def load(self):
        headers = [
            'county',
            'office',
            'district',
            'party',
            'candidate',
            'votes'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['county'].strip() == '':
                    total_votes = int(row['votes'].strip())
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    jurisdiction = row['county'].strip()
                    rr_kwargs.update({
                        'jurisdiction': jurisdiction,
                        'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                            ocd_type_id(jurisdiction)),
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
            'party': row['party'].strip()
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip()
        }
