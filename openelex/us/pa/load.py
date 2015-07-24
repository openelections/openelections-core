import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.insertbuffer import BulkInsertBuffer
from openelex.lib.text import ocd_type_id
from .datasource import Datasource

"""
Pennsylvania elections have pre-processed CSV results files for elections beginning in 2000. These files contain precinct-level data for each of the state's
counties, and includes all contests in that county. Special election results are contained in election-specific files. The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-pa repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if 'special' in election_id:
            loader = CSVSpecialLoader()
        else:
            loader = CSVLoader()
        loader.run(mapping)


class PABaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'USP',
        'USS',
        'USC',
        'STS',
        'STH',
        'AUD',
        'TRE',
        'ATT',
        'GOV',
        'State Senate',
        'State House',
        'State Representative'
    ])

    district_offices = set([
        'USC',
        'STS',
        'STH',
        'State Senate',
        'State House',
        'State Representative'
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

class CSVSpecialLoader(PABaseLoader):
    """
    Loads Pennsylvania special election results from 2000-2012.

    Format:

    Converted CSVs with county-level results.
    """

    def load(self):
        headers = [
            'candidate',
            'office',
            'district',
            'party',
            'county',
            'votes',
            'winner'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        # We use a BulkInsertBuffer because the load process was running out of
        # memory on prod-1
        results = BulkInsertBuffer(RawResult)

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['county'].strip() == '':
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
                        'votes': int(row['votes'].strip())
                    })
                    results.append(RawResult(**rr_kwargs))
            # Flush any remaining results that are still in the buffer and need
            # to be inserted.
            results.flush()

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



class CSVLoader(PABaseLoader):
    """
    Loads Pennsylvania primary and general election results for 2000-2012.

    Format:

    Pennsylvania has tab-delimited files that have been converted to CSV files.
    """

    offices = [
        ('USP', 'President'),
        ('USS', 'U.S. Senate'),
        ('GOV', 'Governor'),
        ('LTG', 'Lieutenant Governor'),
        ('ATT', 'Attorney General'),
        ('AUD', 'Auditor General'),
        ('TRE', 'State Treasurer'),
        ('USC', 'U.S. House'),
        ('STS', 'State Senate'),
        ('STH', 'State Representative')
    ]

    def load(self):
        headers = [
            'year',
            'election_type',
            'county_code',
            'precinct_code',
            'cand_office_rank',
            'cand_district',
            'cand_party_rank',
            'cand_ballot_position',
            'cand_office_code',
            'cand_party_code',
            'cand_number',
            'cand_last_name',
            'cand_first_name',
            'cand_middle_name',
            'cand_suffix',
            'votes',
            'congressional_district',
            'state_senate_district',
            'state_house_district',
            'municipality_type_code',
            'municipality',
            'municipality_breakdown_code_1',
            'municipality_breakdown_name_1',
            'municipality_breakdown_code_2',
            'municipality_breakdown_name_2',
            'bicounty_code',
            'mcd_code',
            'fips_code',
            'vtd_code',
            'previous_precinct_code',
            'previous_congressional_district',
            'previous_state_senate_district',
            'previous_state_house_district'
        ]

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = BulkInsertBuffer(RawResult)

        with self._file_handle as csvfile:
            if '2014' in self.election_id:
                reader = unicodecsv.DictReader((line.replace('\0','') for line in csvfile), fieldnames = headers, encoding='latin-1')
            else:
                reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                if 'primary' in self.mapping['election']:
                    rr_kwargs['primary_party'] = row['cand_party_code'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                county_ocd_id = [c for c in self.datasource._jurisdictions() if c['state_id'] == str(row['county_code'])][0]['ocd_id']
                rr_kwargs.update({
                    'party': row['cand_party_code'].strip(),
                    'jurisdiction': str(row['precinct_code']),
                    'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(str(row['precinct_code']))),
                    'votes': int(row['votes'].strip()),
                    # PA-specific data
                    'congressional_district': row['congressional_district'],
                    'state_senate_district': row['state_senate_district'],
                    'state_house_district': row['state_house_district'],
                    'municipality_type_code': row['municipality_type_code'],
                    'municipality': row['municipality'],
                    'previous_precinct_code': row['previous_precinct_code'],
                    'previous_congressional_district': row['previous_congressional_district'],
                    'previous_state_senate_district': row['previous_state_senate_district'],
                    'previous_state_house_district': row['previous_state_house_district']
                })
                results.append(RawResult(**rr_kwargs))

        results.flush()

    def _skip_row(self, row):
        return row['cand_office_code'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        office = [o for o in self.offices if o[0] == row['cand_office_code']][0][1]
        if row['cand_district'] == 0:
            district = None
        else:
            district = row['cand_district']
        return {
            'office': office,
            'district': district,
        }

    def _build_candidate_kwargs(self, row):
        return {
            'given_name': row['cand_first_name'],
            'family_name': row['cand_last_name'],
            'additional_name': row['cand_middle_name'],
            'suffix': row['cand_suffix']
        }
