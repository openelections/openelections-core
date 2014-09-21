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
        if any(s in election_id for s in ['nc-2008-11-04-general', '2010', '2012']):
            loader = NCCsvLoader()
        elif election_id == 'nc-2008-05-06-primary':
            loader = NCTsv2008Loader()
        elif any(s in election_id for s in ['2004', '2006', '2008']):
            loader = NCTextLoader()
        elif any(s in election_id for s in ['2002', '2000-11-07']):
            loader = NCTsv20022000Loader()
        else:
            loader = NCXlsLoader()
        loader.run(mapping)


class NCBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES',
        'PRESIDENT-VICE PRESIDENT',
        'STRAIGHT PARTY',
        'US HOUSE OF REPRESENTATIVES',
        'US HOUSE OF REP.',
        'US CONGRESS',
        'US SENATE',
        'NC GOVERNOR',
        'GOVERNOR',
        'NC LIEUTENANT GOVERNOR',
        'LIEUTENANT GOVERNOR',
        'NC SECRETARY OF STATE',
        'NC ATTORNEY GENERAL',
        'ATTORNEY GENERAL',
        'NC AUDITOR',
        'AUDITOR',
        'NC COMMISSIONER OF AGRICULTURE',
        'NC COMMISSIONER OF INSURANCE',
        'NC COMMISSIONER OF LABOR',
        'COMMISSIONER OF LABOR',
        'NC SUPERINTENDENT OF PUBLIC INSTRUCTION',
        'SUPER. OF PUBLIC INSTRUCTION',
        'NC TREASURER',
        'TREASURER',
        'NC HOUSE OF REPRESENTATIVES',
        'NC STATE SENATE',
        'SENATE',
        'NC STATE HOUSE',
        'HOUSE',
    ])

    district_offices = set([
        'US HOUSE OF REPRESENTATIVES',
        'US CONGRESS',
        'NC HOUSE OF REPRESENTATIVES',
        'NC STATE SENATE',
        'NC STATE HOUSE',
        'HOUSE',
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
        if val.strip() == '':
            return 0

        try:
            return int(float(val))
        except ValueError:
            # Count'y convert value from string   
            return 0

    def _base_kwargs(self, row):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(row)
        candidate_kwargs = self._build_candidate_kwargs(row)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

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

class NCTsv2008Loader(NCBaseLoader):
    """
    Loads North Carolina 2008 primary tab-delimited results.
    """

    def load(self):
        headers = [
            'county',
            'date',
            'precinct',
            'contest',
            'choice',
            'party',
            'election_day',
            'absentee',
            'provisional',
            'total_votes'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, delimiter='\t', fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                results.append(self._prep_precinct_result(row))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if any(o in row['contest'] for o in self.target_offices):
            return False
        else:
            return True

    def _build_contest_kwargs(self, row):
        if 'DISTRICT' in row['contest']:
            office = row['contest'].split(' DISTRICT ')[0]
            district = row['contest'].split(' DISTRICT ')[1].split(' - ')[0]
        else:
            office = row['contest'].split(' - ')[0]
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

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        precinct = str(row['precinct'])
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes']),
            'vote_breakdowns': self._breakdowns(row, kwargs)
        })
        return RawResult(**kwargs)

    def _breakdowns(self, row, kwargs):
        return { 'election_day': self._votes(row['election_day']), 'absentee_mail': self._votes(row['absentee']), 'provisional': self._votes(row['provisional'])}

class NCTextLoader(NCBaseLoader):
    """
    Loads North Carolina results in tab-delimited format (although 2004 are CSV, but same headers).
    Absentee, provisional and 'transfer' vote totals are also included, but as "precincts" so need to be handled.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []
        with self._file_handle as csvfile:
            if '2004' in self.mapping['election']:
                reader = unicodecsv.DictReader(csvfile, delimiter=',', encoding='latin-1')
            else:
                reader = unicodecsv.DictReader(csvfile, delimiter='\t', encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['precinct'] == 'ABSENTEE' or row['precinct'] == 'PROV':
                    results.append(self._prep_county_result(row))
                else:
                    results.append(self._prep_precinct_result(row))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if any(o in row['contest_name'] for o in self.target_offices):
            return False
        else:
            return True

    def _build_contest_kwargs(self, row):
        if 'DISTRICT' in row['contest_name']:
            office = row['contest_name'].split(' DISTRICT ')[0].strip()
            district = row['contest_name'].split(' DISTRICT ')[1].split(' - ')[0].strip()
        else:
            office = row['contest_name'].split(' - ')[0].strip()
            district = None
        kwargs = {
            'office': office,
            'district': district,
            'primary_party': row['party_cd'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['name_on_ballot'].strip()
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        precinct = str(row['precinct']).strip()
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['party_cd'].strip(),
            'votes': self._votes(row['ballot_count'])
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': row['county'],
            'ocd_id': county_ocd_id,
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes'])
        })
        return RawResult(**kwargs)

class NCTsv20022000Loader(NCBaseLoader):
    """
    Loads North Carolina 2002 primary and general, plus 2000 general tab-delimited precinct-level results. Absentee/provisional totals are 
    at the county level.
    """

    def load(self):
        headers = [
            'county',
            'date',
            'precinct_abbrev',
            'precinct',
            'contest',
            'choice',
            'party',
            'total_votes',
            'timestamp'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, delimiter='\t', fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['precinct'] == 'absentee/provisional':
                    results.append(self._prep_county_result(row))
                else:
                    results.append(self._prep_precinct_result(row))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if any(o in row['contest'] for o in self.target_offices):
            return False
        else:
            return True

    def _build_contest_kwargs(self, row):
        if 'DISTRICT' in row['contest'] and row['date'] == '11/07/2000':
            office = row['contest'].split(' DISTRICT ')[0].strip()
            district = row['contest'].split(' DISTRICT ')[1]
        elif 'DISTRICT' in row['contest']:
            office = row['contest'].split('(')[0].strip()
            district = row['contest'].split('(')[1].split(' ')[0]
        elif row['contest'][0:2] == 'NC':
            office = contest.split('(')[0].strip()
            district = row['contest'].split('(')[1].split(')')[0]
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

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        precinct = str(row['precinct']).strip()
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes'])
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': row['county'],
            'ocd_id': county_ocd_id,
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes'])
        })
        return RawResult(**kwargs)