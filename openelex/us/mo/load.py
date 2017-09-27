from __future__ import print_function
from builtins import object
import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Missouri elections have pre-processed CSV results files for elections beginning in 2000. These files contain
county-level data for all of the state's counties. Special election results are contained in office-specific
files. The CSV versions of those are contained in the https://github.com/openelections/openelections-data-mo repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['generated_filename']
        if 'precinct' in election_id:
            loader = MOPrecinctLoader()
        elif 'special' in election_id:
            loader = MOSpecialLoader()
        else:
            loader = MOCountyLoader()
        loader.run(mapping)


class MOBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President',
        'U.S. Senate',
        'U.S. House',
        'Governor',
        'Lieutenant Governor',
        'State Treasurer',
        'State Auditor',
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

class MOPrecinctLoader(MOBaseLoader):
    """
    Loads Missouri precinct results.

    Format:

    """

    def load(self):
        headers = [
            'county',
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
                if row['precinct'].strip() == '':
                    total_votes = int(row['votes'].strip())
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs['primary_party'] = row['party'].strip()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    jurisdiction = row['precinct'].strip()
                    print(row['county'])
                    county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': jurisdiction,
                        'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(jurisdiction)),
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
        if 'candidate' in row:
            return {
                'full_name': row['candidate'].strip()
            }
        else:
            return {
                'first_name': row['first_name'].strip(),
                'last_name': row['last_name'].strip(),
                'full_name': row['first_name']+' '+row['last_name']
            }

class MOCountyLoader(MOBaseLoader):
    """
    Loads Missouri county-level results for 2000-2014 primary and general elections.

    Format:

    Missouri has PDF files that have been converted to CSV files for elections from 2000.
    Header rows are identical except that precinct is not present.
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
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                if row['county'].strip() == '':
                    rr_kwargs.update({
                        'contest_winner': row['winner'].strip(),
                        'write_in': row['write-in'],
                        'notes': row['notes']
                    })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['office'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                ocd_type_id(row['county'].strip())),
            'jurisdiction': row['county'].strip(),
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip(),
            'votes': int(row['votes'].strip())
        }

class MOSpecialLoader(MOBaseLoader):
    """
    Loads Missouri results for 2000-2014 special elections.

    Format:

    Missouri has PDF files that have been converted to CSV files for elections after 2000.
    Header rows are identical except that precinct is not present.
    """

    def load(self):
        headers = [
            'county',
            'office',
            'district',
            'party',
            'candidate',
            'votes',
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'state'
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
                if row['county'].strip() == '':
                    jurisdiction = "Missouri"
                    ocd_id = "ocd-division/country:us/state:mo"
                    reporting_level = 'state'
                else:
                    jurisdiction = row['county'].strip()
                    ocd_id = "{}/county:{}".format(self.mapping['ocd_id'],
                        ocd_type_id(jurisdiction))
                    reporting_level = 'county'
                rr_kwargs.update({
                    'reporting_level': reporting_level,
                    'jurisdiction': jurisdiction,
                    'ocd_id': ocd_id,
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
