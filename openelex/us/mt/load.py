import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Montana elections have pre-processed CSV county-level results files for elections beginning in 2000. Files are in the
https://github.com/openelections/openelections-data-mt repository. There also are some precinct-level files in both
Excel and PDF formats on the state election division site.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['pre_processed_url']
        if 'precinct' in election_id:
            loader = MTPrecinctLoader()
        else:
            loader = MTCountyLoader()
        loader.run(mapping)


class MTBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President',
        'U.S. Senate',
        'U.S. House',
        'Governor',
        'Secretary of State',
        'State Auditor',
        'Attorney General',
        'Superintendent of Public Instruction',
        'Clerk of Supreme Court',
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

class MTPrecinctLoader(MTBaseLoader):
    """
    Loads Montana precinct-level results.

    """

    def load(self):
        headers = [
            'candidate',
            'office',
            'district',
            'party',
            'county',
            'precinct',
            'votes',
            'winner'
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
                    contest_winner = row['winner'].strip()
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs['primary_party'] = row['party'].strip()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    jurisdiction = row['precinct'].strip()
                    county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': jurisdiction,
                        'parent_jurisdiction': row['county'],
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
        return {
            'full_name': row['candidate'].strip()
        }

class MTCountyLoader(MTBaseLoader):
    """
    Loads Montana county-level results for 2003-2013 primary and general elections.

    Format:

    Montana has PDF files that have been converted to CSV files for elections after 2000.
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
                if 'TOTAL' in row['county'].upper():
                    votes = int(row['votes'].strip())
                    ocd_id = None
                    jurisdiction = "Total"
                else:
                    if row['votes'].strip() == 'Withdrew':
                        votes = None
                    else:
                        votes = int(row['votes'].strip())
                    jurisdiction = row['county'].strip()
                    print row['county']
                    ocd_id = [o['ocd_id'] for o in self.datasource._jurisdictions() if row['county'].strip() == o['county']][0]
                rr_kwargs.update({
                    'party': row['party'].strip(),
                    'jurisdiction': jurisdiction,
                    'ocd_id': ocd_id,
                    'office': row['office'].strip(),
                    'district': row['district'].strip(),
                    'votes': votes
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
