import re
import csv
import xlrd
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
North Carolina elections have a mixture of CSV, tab-delimited text and Excel files for results. These files contain precinct-level data for each of the state's
counties, and includes all contests in that county.

Although some of the CSV files have a `district` column, the district information is contained in the `contest` column and needs to be parsed out. The
Excel files cover separate offices and have sheets for individual contests. CSV files also have totals for one-stop, absentee, provisional and
transfer votes, which appear as "precincts" in the data.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if any(s in election_id for s in ['2014']):
            loader = NCTsv2014Loader()
        elif any(s in election_id for s in ['nc-2008-11-04-general', '2010', '2012']):
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
        'US CONGRESS DISTRICT',
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
        'COMMISSIONER OF AGRICULTURE',
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
        if type(val) is str:
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
        return kwargs

class NCTsv2014Loader(NCBaseLoader):
    """
    Loads North Carolina results in tab-delimited format.
    Absentee, provisional and 'transfer' vote totals are also included, but as "precincts" so need to be handled.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []
        with self._file_handle as tsvfile:
            tsv = [x.replace('\0', '') for x in tsvfile] # remove NULL bytes
            reader = unicodecsv.DictReader(tsv, delimiter='\t', encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                if row['Precinct'] in ('CURBSIDE', 'PROVISIONAL', 'ABSENTEE BY MAIL', 'ONESTOP', 'TRANSFER'):
                    results.append(self._prep_county_result(row))
                else:
                    results.append(self._prep_precinct_result(row))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if any(o in row['Contest Name'] for o in self.target_offices):
            return False
        else:
            return True

    def _build_contest_kwargs(self, row):
        if 'DISTRICT' in row['Contest Name']:
            office = row['Contest Name'].split(' DISTRICT ')[0].strip()
            district = row['Contest Name'].split(' DISTRICT ')[1].split(' - ')[0].split(' ')[0]
        else:
            office = row['Contest Name'].split(' - ')[0].strip()
            district = None
        kwargs = {
            'office': office,
            'district': district,
            'primary_party': row['Choice Party'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['Choice'].strip()
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update(self._build_contest_kwargs(row))
        kwargs.update(self._build_candidate_kwargs(row))
        precinct = str(row['Precinct']).strip()
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['County'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': row['County'],
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['Choice Party'].strip(),
            'votes': self._votes(row['Total Votes']),
            'vote_breakdowns': self._breakdowns(row)
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update(self._build_contest_kwargs(row))
        kwargs.update(self._build_candidate_kwargs(row))
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['County'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': row['County'],
            'ocd_id': county_ocd_id,
            'party': row['Choice Party'].strip(),
            'votes_type': row['Precinct'],
            'votes': self._votes(row['Total Votes'])
        })
        return RawResult(**kwargs)

    def _breakdowns(self, row):
        return { 'election_day': self._votes((row['Election Day'])), 'absentee_by_mail': self._votes(row['Absentee by Mail']), 'one_stop': self._votes(row['One Stop']), 'provisional': self._votes(row['Provisional'])}

class NCCsvLoader(NCBaseLoader):
    """
    Parse North Carolina election results in CSV format.
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
        if " ".join(row['contest'].split(' ')[:3]) in self.target_offices:
            return False
        else:
            return True

    def _build_contest_kwargs(self, row):
        if 'DISTRICT' in row['contest']:
            try:
                office, district = row['contest'].split(' DISTRICT ')
            except:
                print row['contest']
                raise
        else:
            office = row['contest'].strip()
            district = None
        if 'primary' in self.source:
            party = row['party'].strip()
        else:
            party = None
        kwargs = {
            'office': office,
            'district': district,
            'primary_party': party
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['choice'].strip()
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            'name_slug': slug,
        }
        return kwargs

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update(self._build_contest_kwargs(row))
        kwargs.update(self._build_candidate_kwargs(row))
        precinct = str(row['precinct'])
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': row['county'],
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
        if " ".join(row['contest'].split(' ')[:3]) in self.target_offices:
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
        kwargs.update(self._build_contest_kwargs(row))
        kwargs.update(self._build_candidate_kwargs(row))
        precinct = str(row['precinct'])
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': row['county'],
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
        kwargs.update(self._build_contest_kwargs(row))
        kwargs.update(self._build_candidate_kwargs(row))
        precinct = str(row['precinct']).strip()
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': row['county'],
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
        if " ".join(row['contest'].split(' ')[:3]) in self.target_offices:
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
            office = row['contest'].split('(')[0].strip()
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
            'parent_jurisdiction': row['county'],
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes'])
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == row['county'].upper()][0]['ocd_id']
        if row['precinct'] == 'absentee/provisional':
            votes_type = 'absentee_provisional'
        else:
            votes_type = None
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': row['county'],
            'ocd_id': county_ocd_id,
            'party': row['party'].strip(),
            'votes': self._votes(row['total_votes']),
            'votes_type': votes_type,
        })
        return RawResult(**kwargs)

class NCXlsLoader(NCBaseLoader):
    """
    Loads North Carolina 2000 primary results, which are contained in office-specific Excel files. For district-level
    offices, each district is represented on a separate worksheet.
    """

    def load(self):
        headers = [
            'county',
            'precinct',
            'contest',
            'choice',
            'party',
            'total_votes',
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        xlsfile = xlrd.open_workbook(self._xls_file_path)
        if 'house' in self.source or 'state_senate' in self.source:
            sheets = xlsfile.sheets()
        elif 'republican__primary__lieutenant_governor' in self.source:
            sheets = [xlsfile.sheets()[5]]
        else:
            sheets = [xlsfile.sheets()[0]]

        for sheet in sheets:
            office, district = self._detect_office(sheet)
            if sheet.name == '83rd NC House':
                cands = [c for c in sheet.row_values(1)[2:] if c != '']
                parties = [x.replace('(','').replace(')','') for x in sheet.row_values(2)[2:] if x != '']
                start_row = 3
            elif sheet.name == '97th NC House':
                cands = [c for c in sheet.row_values(2)[2:] if c != '']
                parties = [x.replace('(','').replace(')','') for x in sheet.row_values(3)[2:] if x != '']
                start_row = 4
            elif sheet.row_values(0)[1].upper() == 'PRECINCT' or sheet.row_values(0)[2] == 'John Cosgrove' or sheet.row_values(0)[1].upper() == 'PRECINCTS' or sheet.row_values(0)[2] == 'Paul Luebke' or sheet.row_values(0)[1] == 'Precinct Name':
                cands = [c for c in sheet.row_values(0)[2:] if c != '']
                parties = [x.replace('(','').replace(')','') for x in sheet.row_values(1)[2:] if x != '']
                start_row = 2
            else:
                cands = [c for c in sheet.row_values(2)[2:] if c != '']
                parties = [x.replace('(','').replace(')','') for x in sheet.row_values(3)[2:] if x != '']
                start_row = 2
            candidates = zip(cands, parties)
            for i in xrange(start_row, sheet.nrows):
                row = [r for r in sheet.row_values(i)]
                if self._skip_row(row):
                    continue
                for idx, cand in enumerate(candidates):
                    if row[1] == '':
                        county = row[0]
                        results.append(self._prep_county_result(row, office, district, cand, county, row[idx+2]))
                    else:
                        results.append(self._prep_precinct_result(row, office, district, cand, county, row[idx+2]))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if row == []:
            return True
        elif row[0] == '' and row[1] == '':
            return True
        elif row[0] == ' ' and row[1] == '':
            return True
        elif row[0].upper() == 'TOTAL':
            return True
        elif row[0] == 'County':
            return True
        else:
            return False

    def _detect_office(self, sheet):
        district = None
        if 'state_house' in self.source:
            office = 'NC HOUSE OF REPRESENTATIVES'
            district = sheet.name.split(' ')[0]
        elif 'state_senate' in self.source:
            office = 'NC STATE SENATE'
            district = sheet.name.split(' ')[0]
        elif 'house' in self.source:
            office = 'US HOUSE OF REPRESENTATIVES'
            district = sheet.name.split(' ')[0]
        elif 'lieutenant_governor' in self.source:
            office = 'LIEUTENANT GOVERNOR'
        elif 'governor' in self.source:
            office = 'GOVERNOR'
        elif 'auditor' in self.source:
            office = 'AUDITOR'
        elif 'commissioner_of_agriculture' in self.source:
            office = 'COMMISSIONER OF AGRICULTURE'
        elif 'commissioner_of_labor' in self.source:
            office = 'COMMISSIONER OF LABOR'
        elif 'treasurer' in self.source:
            office = 'TREASURER'
        elif 'president' in self.source:
            office = 'PRESIDENT-VICE PRESIDENT'
        return [office, district]

    def _build_contest_kwargs(self, office, district, party):
        kwargs = {
            'office': office,
            'district': district,
            'primary_party': party,
        }
        return kwargs

    def _build_candidate_kwargs(self, candidate):
        full_name = candidate[0]
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs

    def _prep_precinct_result(self, row, office, district, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        precinct = str(row[1]).strip()
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': county,
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct)),
            'party': candidate[1],
            'votes': self._votes(votes)
        })
        return RawResult(**kwargs)

    def _prep_county_result(self, row, office, district, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': county,
            'ocd_id': county_ocd_id,
            'party': candidate[1],
            'votes': self._votes(votes)
        })
        return RawResult(**kwargs)

    def _base_kwargs(self, row, office, district, candidate):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(office, district, candidate[1])
        candidate_kwargs = self._build_candidate_kwargs(candidate)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs
