from builtins import object
import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Texas elections have pre-processed CSV results files for elections beginning in 2000. These files contain statewide and county-level data
for each election. The CSV versions are contained in the https://github.com/openelections/openelections-data-tx repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['pre_processed_url']
        if 'precinct' in election_id:
            loader = TXPrecinctLoader()
        elif 'county' in election_id:
            loader = TXCountyLoader()
        else:
            loader = TXLoader()
        loader.run(mapping)


class TXBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President/VicePresident',
        'U.S. Senator',
        'U. S. Representative',
        'Governor',
        'Lieutenant Governor',
        'Railroad Commissioner',
        'Comptroller of Public Accounts',
        'Commissioner of the General Land Office',
        'Commissioner of Insurance',
        'Commissioner of Agriculture',
        'Attorney General',
        'State Senator',
        'State Representative',
    ])

    district_offices = set([
        'State Senator',
        'State Representative',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

    def _votes(self, val):
        """
        Returns cleaned version of votes or 0 if it's a non-numeric value.
        """
        if type(val) is str:
            if val.strip() == '':
                return 0

        try:
            return int(float(val))
        except ValueError:
            # Count'y convert value from string
            return 0

class TXPrecinctLoader(TXBaseLoader):
    """
    Loads Texas precinct-level results.

    """

    def load(self):
        headers = [
            'county',
            'precinct',
            'office',
            'district',
            'party',
            'candidate',
            'votes',
            'pct'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames=headers)
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs['primary_party'] = row['party'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = row['precinct'].strip()
                county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
                rr_kwargs.update({
                    'jurisdiction': jurisdiction,
                    'parent_jurisdiction': row['county'],
                    'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(jurisdiction)),
                    'votes': self._votes(row['votes'])
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
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip(),
        }

class TXCountyLoader(TXBaseLoader):
    """
    Loads Texas county-level results.
    """

    def load(self):
        headers = [
            'county',
            'office',
            'district',
            'candidate',
            'incumbent',
            'party',
            'votes',
            'pct'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames=headers)
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = row['county'].strip()
                rr_kwargs.update({
                    'jurisdiction': jurisdiction,
                    'parent_jurisdiction': "Texas",
                    'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                        ocd_type_id(jurisdiction)),
                    'votes': self._votes(row['votes'])
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
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip(),
        }

class TXLoader(TXBaseLoader):
    """
    Loads Texas statewide results.

    """

    def load(self):
        headers = [
            'office',
            'district',
            'candidate',
            'incumbent',
            'party',
            'votes',
            'pct'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames=headers)
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                rr_kwargs.update({
                    'jurisdiction': "Texas",
                    'ocd_id': "ocd-division/country:us/state:tx",
                    'votes': self._votes(row['votes'])
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
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip()
        }
