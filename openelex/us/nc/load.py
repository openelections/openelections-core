import re
import csv
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
North Carolina elections have a mixture of CSV, tab-delimited text and Excel files for results. These files contain precinct-level data for each of the state's
counties, and includes all contests in that county.

Although the CSV files have a `district` column, the district information is contained in the `contest` column and needs to be parsed out. The 
Excel files cover separate offices and have sheets for individual contests. CSV files also have totals for one-stop, absentee, provisional and 
transfer votes, which appear as "precincts" in the data.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if any(s in election_id for s in ['20081104__nc__general__precinct', '2010', '2012']):
            loader = NCCsvLoader()
        elif '2004' in election_id:
            loader = NCCsv2004Loader()
        elif any(s in election_id for s in ['2002', '2006', '2000-11-07', '2008']):
            loader = NCTextLoader()
        else:
            loader = NCXlsLoader()
        loader.run(mapping)


class NCBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES',
        'STRAIGHT PARTY',
        'US HOUSE OF REPRESENTATIVES',
        'US CONGRESS',
        'US SENATE',
        'NC GOVERNOR',
        'NC LIEUTENANT GOVERNOR',
        'NC SECRETARY OF STATE',
        'NC ATTORNEY GENERAL',
        'NC AUDITOR',
        'NC COMMISSIONER OF AGRICULTURE',
        'NC COMMISSIONER OF INSURANCE',
        'NC COMMISSIONER OF LABOR',
        'NC SUPERINTENDENT OF PUBLIC INSTRUCTION',
        'NC TREASURER',
        'NC HOUSE OF REPRESENTATIVES',
        'NC STATE SENATE',
    ])

    district_offices = set([
        'US HOUSE OF REPRESENTATIVES',
        'US CONGRESS',
        'NC HOUSE OF REPRESENTATIVES',
        'NC STATE SENATE',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False


class NCCsvLoader(NCBaseLoader):
    """
    Parse North Carolina election results in CSV format

    """
    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if self._skip_row(row): 
                    continue
                results.append(self._prep_precinct_result(row))
            RawResult.objects.insert(results)

    def _skip_row(self, row):
        if any(o in row['contest'] for o in self.target_offices):
            return False
        else:
            return True

    def _build_contest_kwargs(self, row, primary_type):
        if 'DISTRICT' in row['contest']:
            office, district = row['contest'].split(' DISTRICT ')
        else:
            office = row['contest'].strip()
            district = None
        kwargs = {
            'office': office,
            'district': district,
            'primary_party': row['party'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['choice'].strip()
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
        precinct = str(row['precinct'])
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['party'].strip(),
            'votes': self._votes(row['total votes']),
            'vote_breakdowns': self._breakdowns(row, kwargs),
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

    def _breakdowns(self, row, kwargs):
        if any(s in kwargs['election_id'] for s in ['2010', '2012']):
            breakdows = { 'election_day': self._votes((row['Election Day'])), 'one_stop': self._votes(row['One Stop']), 'absentee_mail': self._votes(row['Absentee by Mail']), 'provisional': self._votes(row['Provisional'])}
        else:
            breakdows = { 'election_day': self._votes((row['Election Day'])), 'absentee_onestop': self._votes(row['Absentee / One Stop']), 'provisional': self._votes(row['Provisional'])}
        return breakdows

    def _writein(self, row):
        # sometimes write-in field not present
        try:
            write_in = row['Write-In?'].strip()
        except KeyError:
            write_in = None
        return write_in


class NCTextLoader(NCBaseLoader):
    """
    Loads North Carolina results in tab-delimited format.

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
