import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import slugify
from .datasource import Datasource

"""
Wyoming elections have CSV results files for elections in 2006, along with special elections in 2008 and 2002, 
contained in the https://github.com/openelections/openelections-data-wy repository. Other results files are in Excel
format, contained in zip files or in converted spreadsheets in the same Github repository. These files have multiple
worksheets for primaries, one for each party.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if any(s in election_id for s in ['2006', 'special']):
            loader = WYLoaderCSV()
        else:
            loader = WYLoader()
        loader.run(mapping)


class WYBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'U.S. President',
        'U.S. Senate',
        'U.S. House',
        'Governor',
        'Secretary of State',
        'State Auditor',
        'State Treasurer',
        'Superintendent of Public Instruction',
        'State Senate',
        'State House',
    ])

    district_offices = set([
        'U.S. House',
        'United States Representative'
        'State Senate',
        'State House',
        'House'
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False


class WYLoader(WYBaseLoader):
    """
    Parse Wyoming election results for all elections except those in 2006 or special elections.

    """
    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if self._skip_row(row): 
                    continue
                else:
                    results.append(self._prep_precinct_result(row))
            RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['OfficeDescription'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row, primary_type):
        kwargs = {
            'office': row['OfficeDescription'].strip(),
            'district': row['District'].strip(),
            'primary_party': row['PartyName'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['Name'].strip()
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs

    def _base_kwargs(self, row):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(row, kwargs['primary_type'])
        candidate_kwargs = self._build_candidate_kwargs(row)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        precinct = str(row['Precinct'])
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            # In West Virginia, precincts are nested below counties.
            #
            # The mapping ocd_id will be for the precinct's county.
            # We'll save it as an expando property of the raw result because
            # we won't have an easy way of looking up the county in the 
            # transforms.
            'county_ocd_id': self.mapping['ocd_id'],
            'party': row['PartyName'].strip(),
            'votes': self._votes(row['Votes']),
            'vote_breakdowns': {},
        })
        return RawResult(**kwargs)

    def _votes(self, val):
        """
        Returns cleaned version of votes or 0 if it's a non-numeric value.
        """
        if val.strip() == '':
            return 0

        try:
            return int(float(val))
        except ValueError:
            # Count'y convert value from string   
            return 0

    def _writein(self, row):
        # sometimes write-in field not present
        try:
            write_in = row['Write-In?'].strip()
        except KeyError:
            write_in = None
        return write_in


class WYLoaderCSV(WYBaseLoader):
    """
    Loads Wyoming results for 2006 and for special elections.

    Format:

    Wyoming has PDF files that have been converted to CSV files with office names that correspond
    to those used for elections in 2006 and for special elections.
    """

    def load(self):
        headers = [
            'office',
            'party',
            'district',
            'candidate',
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
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs['primary_party'] = row['party'].strip()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': row['precinct'].strip(),
                        'votes': int(row['votes'].strip()),
                        'winner': row['winner'].strip(),
                        'county_ocd_id': self.mapping['ocd_id'],
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
