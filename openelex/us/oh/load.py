import re
import csv
import unicodecsv
import xlrd

from bs4 import BeautifulSoup

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if '2014' in election_id and 'general' in election_id:
            loader = OH2014CountyLoader()
        elif 'precinct' in election_id:
            loader = OHPrecinctLoader()
        elif '2000' in election_id and 'primary' in election_id:
            loader = OHLoader2000Primary()
        elif '2008' in election_id and 'special' in election_id:
            loader = OHLoader2008Special()
        else:
            pass
            #loader = OHHTMLoader()
        loader.run(mapping)


class OHBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President - Vice Pres',
        'President and Vice President of the United States',
        'U.S. Senate',
        'U.S. Representative',
        'U.S. House of Representatives',
        'Representative in Congress',
        'Governor/Lieutenant Governor',
        'Governor',
        'Attorney General',
        'Auditor of State',
        'Secretary of State',
        'Treasurer of State',
        'State Senate',
        'State Representative',
        'State House of Representatives',
    ])

    district_offices = set([
        'U.S. Congress',
        'Representative in Congress',
        'State Senator',
        "House of Delegates",
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

class OH2014CountyLoader(OHBaseLoader):
    """
    Parse Ohio election results for 2014 county-level xlsx files.
    """

    def load(self):
        workbook = xlrd.open_workbook(self._xls_file_path)
        results = []
#        workbook = xlrd.open_workbook(xlsfile)
        worksheet = workbook.sheet_by_name('Master')
        raw_offices = [c.value.strip() for c in worksheet.row(0)[5:]]
        last_office_column = self._get_last_office_column(raw_offices)
        offices = self._get_offices(raw_offices, last_office_column)
        candidates = [c.value for c in worksheet.row(1)[5:last_office_column+7]]
        combined = zip(offices, candidates)
        for i in range(4, worksheet.nrows):
            row = [c.value for c in worksheet.row(i)[:last_office_column+5]]
            county = row[0]
            county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
            results = row[5:last_office_column+5]
            for result in zip(combined, results):
                if result[1] == 0.0:
                    continue
                office, candidate = result[0]
                cand, party = candidate.split(' (')
                party = party.replace(')','')
                votes = result[1]
                kwargs = self._base_kwargs(row)
                slug = slugify(cand, substitute='-')
                kwargs.update(
                    {'office': office,
                    'district': district,
                    'full_name': cand,
                    'slug': slug,
                    'party': party,
                    'votes': votes,
                    'reporting_level': 'county',
                    'jurisdiction': county,
                    'ocd_id': county_ocd_id,
                })
                results.append(kwargs)
            RawResult.objects.insert(results)

    def _get_offices(self, raw_offices, last_office_column):
        new_offices = []
        offices = raw_offices[:last_office_column+2]
        for office in offices:
          if office != '':
            previous_office = office
          elif office == '':
            office = previous_office
          new_offices.append(office)
        return new_offices

    def _get_last_office_column(self, raw_offices):
        for index, item in enumerate(reversed(raw_offices)):
            if 'State House of Representatives' in item:
                return len(raw_offices) - index - 1

    def _base_kwargs(self, row):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        return kwargs


class OH2012PrecinctLoader(OHBaseLoader):
    """
    Parse Ohio election results for 2012 precinct-level results files.
    """
    def load(self):
        with self._file_handle as xlsfile:
            results = []
            workbook = xlrd.open_workbook(xlsfile)
            worksheet = workbook.sheet_by_name('AllCounties')
            headers = worksheet.row(1)

            for row in reader:
                # Skip non-target offices
                if self._skip_row(row):
                    continue
                elif 'state_legislative' in self.source:
                    results.extend(self._prep_state_leg_results(row))
                elif 'precinct' in self.source:
                    results.append(self._prep_precinct_result(row))
                else:
                    results.append(self._prep_county_result(row))
            RawResult.objects.insert(results)



class OH2010PrecinctLoader(OHBaseLoader):
    """
    Parse Ohio election results for 2010 precinct-level results (general
    and primary) contained in xlsx/xls files.
    """
    def load(self):
        with self._file_handle as xlsfile:
            results = []
            workbook = xlrd.open_workbook(xlsfile)
            worksheet = workbook.sheet_by_name('AllCounties')
            headers = worksheet.row(1)

            for row in reader:
                # Skip non-target offices
                if self._skip_row(row):
                    continue
                elif 'state_legislative' in self.source:
                    results.extend(self._prep_state_leg_results(row))
                elif 'precinct' in self.source:
                    results.append(self._prep_precinct_result(row))
                else:
                    results.append(self._prep_county_result(row))
            RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['Office Name'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row, primary_type):
        kwargs = {
            'office': row['Office Name'].strip(),
            'district': row['Office District'].strip(),
        }
        # Add party if it's a primary
        #TODO: QUESTION: Should semi-closed also have party?
        if primary_type == 'closed':
            kwargs['primary_party'] = row['Party'].strip()
        return kwargs

    def _build_candidate_kwargs(self, row):
        try:
            full_name = row['Candidate Name'].strip()
        except KeyError:
            # 2000 results use "Candidate" for the column name
            full_name = row['Candidate'].strip()
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

    def _get_state_ocd_id(self):
        # It looks like the OCD ID in this years mappings is
        # ocd-division/country:us/state:oh/precinct:all
        # We need to get rid of the "precinct:all" part to
        # build valid OCD IDs for the individual jurisdictions.
        return '/'.join(self.mapping['ocd_id'].split('/')[:-1])

    def _prep_state_leg_results(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update({
            'reporting_level': 'state_legislative',
            'winner': row['Winner'].strip(),
            'write_in': self._writein(row),
            'party': row['Party'].strip(),
        })
        try:
            kwargs['write_in'] = row['Write-In?'].strip() # at the contest-level
        except KeyError as e:
            pass
        results = []
        for field, val in row.items():
            clean_field = field.strip()
            # Legislative fields prefixed with LEGS
            if not clean_field.startswith('LEGS'):
                continue

            kwargs.update({
                'jurisdiction': clean_field,
                'ocd_id': "{}/sldl:{}".format(self._get_state_ocd_id(),
                    ocd_type_id(clean_field)),
                'votes': self._votes(val),
            })
            results.append(RawResult(**kwargs))
        return results

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        vote_brkdown_fields = [
           ('election_night_total', 'Election Night Votes'),
           ('absentee_total', 'Absentees Votes'),
           ('provisional_total', 'Provisional Votes'),
           ('second_absentee_total', '2nd Absentees Votes'),
        ]
        vote_breakdowns = {}
        for field, key in vote_brkdown_fields:
            try:
                vote_breakdowns[field] = row[key].strip()
            except KeyError:
                pass
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': self.mapping['name'],
            'ocd_id': "{}/county:{}".format(self._get_state_ocd_id(),
                ocd_type_id(self.mapping['name'])),
            'party': row['Party'].strip(),
            'votes': self._votes(row['Total Votes']),
        })
        if (kwargs['office'] not in self.district_offices
                and kwargs['district'] != ''):
            kwargs['reporting_level'] = 'congressional_district_by_county'
            kwargs['reporting_district'] = kwargs['district']
            del kwargs['district']

        return RawResult(**kwargs)

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        vote_breakdowns = {
            'election_night_total': self._votes(row['Election Night Votes'])
        }
        precinct = "%s-%s" % (row['Election District'], row['Election Precinct'].strip())
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'ocd_id': "{}/precinct:{}".format(self._get_state_ocd_id(),
                ocd_type_id(precinct)),
            'party': row['Party'].strip(),
            'votes': self._votes(row['Election Night Votes']),
            'winner': row['Winner'],
            'write_in': self._writein(row),
            'vote_breakdowns': vote_breakdowns,
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
