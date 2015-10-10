import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
West Virginia elections have CSV results files for elections after 2006. These files contain precinct-level data for each of the state's
counties, and includes all contests in that county. Prior to 2008, county-level results are contained in office-specific PDF files. The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-wv repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if any(s in election_id for s in ['2000', '2002', '2004', '2006']):
            loader = WVLoaderPre2008()
        else:
            loader = WVLoader()
        loader.run(mapping)


class WVBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'U.S. President',
        'U.S. Senate',
        'U.S. House of Representatives',
        'Governor',
        'Secretary of State',
        'Auditor',
        'State Treasurer',
        'Commissioner of Agriculture',
        'Attorney General',
        'State Senate',
        'House of Delegates',
    ])

    district_offices = set([
        'U.S. House of Representatives',
        'State Senate',
        'House of Delegates',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False


class WVLoader(WVBaseLoader):
    """
    Parse West Virginia election results for all elections after 2006.

    """
    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if self._skip_row(row):
                    continue
                elif any(s in self.mapping['generated_filename'] for s in ['2008', '2010', '2011']):
                    if row['Type'] == 'County':
                        results.append(self._prep_county_result(row))
                    else:
                        continue
                else:
                    results.append(self._prep_precinct_result(row))
            RawResult.objects.insert(results)

    def _skip_row(self, row):
        if row['OfficialResults'] and row['OfficialResults'] == 'No':
            return True
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
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['CountyName'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': row['CountyName'],
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['PartyName'].strip(),
            'votes': self._votes(row['Votes']),
            'vote_breakdowns': {},
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'] == row['CountyName']][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': row['CountyName'],
            'ocd_id': county_ocd_id,
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


class WVLoaderPre2008(WVBaseLoader):
    """
    Loads West Virginia results for 2000-2006.

    Format:

    West Virginia has PDF files that have been converted to CSV files with office names that correspond
    to those used for elections after 2006. Header rows are identical except for statewide offices that
    do not contain districts.
    """

    def load(self):
        headers = [
            'year',
            'election',
            'office',
            'party',
            'district',
            'candidate',
            'county',
            'votes',
            'winner'
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
                if row['county'].strip() == 'Totals':
                    total_votes = int(row['votes'].strip())
                    contest_winner = row['winner'].strip()
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs['primary_party'] = row['party'].strip()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    jurisdiction = row['county'].strip()
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': jurisdiction,
                        'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                            ocd_type_id(jurisdiction)),
                        'office': row['office'].strip(),
                        'district': row['district'].strip(),
                        'votes': int(row['votes'].strip()),
                        'winner': row['winner'].strip(),
                        'total_votes': total_votes,
                        'contest_winner': contest_winner
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
