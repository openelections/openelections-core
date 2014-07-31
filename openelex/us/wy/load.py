import re
import xlrd
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
        'United States President',
        'U.S. Senate',
        'United States Senator',
        'U.S. House',
        'United States Representative',
        'Governor',
        'Secretary of State',
        'State Auditor',
        'State Treasurer',
        'Superintendent of Public Instruction',
        'State Senate',
        'State House',
        'House District',
        'Senate District',
    ])

    office_segments = set([
        'United States',
        'House District',
        'Senate District',
        'Governor',
        'Secretary of',
        'State Auditor',
        'State Superintendent',
    ])

    district_offices = set([
        'U.S. House',
        'United States Representative'
        'State Senate',
        'State House',
        'House District',
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
        xlsfile = xlrd.open_workbook(self._xls_file_handle())
        results = []
        sheet = xlsfile.sheet_by_name('Sheet1')
        candidates = self._build_candidates(sheet)

        for i in xrange(sheet.nrows):
            row = sheet.row_values(i)
            precinct = str(row[0])
            # Skip non-target offices
            if self._skip_row(row): 
                continue
            else:
                votes = [v for v in row[1:len(candidates)] if not v == '']
                grouped_results = zip(candidates, votes)
                for (candidate, office, party), votes in grouped_results:
                    if not votes == '-':
                        results.append(self._prep_precinct_result(precinct, candidate, office, party, votes))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if row[0] == 'Total':
            return True
        # if the contents of the second cell is not a float, skip that row
        try:
            float(row[1])
            return False
        except ValueError:
            return True

    def _build_offices(self, sheet):
        if sheet.row_values(0)[1] != '':
            offices = sheet.row_values(0)[1:]
        else:
            offices = sheet.row_values(1)
        office_indexes = [offices.index(x) for x in offices if x != '' and " ".join(x.split()[0:2]).strip() in self.office_segments]
        office_labels = [x for x in offices if " ".join(x.split()[0:2]).strip() in self.office_segments]
        new_offices = []
        for o in offices:
            if o in office_labels:
                previous = o
                new_offices.append(o)
            elif o == '':
                new_offices.append(previous)
            else:
                break
        return new_offices

    def _build_candidates(self, sheet):
        # map candidates to offices so we can lookup up one and get the other
        # for our purposes, candidates include totals for write-ins, over and under votes
        # TODO: filter out write-ins, over and under votes
        offices = self._build_offices(sheet)
        if 'Republican' in sheet.row_values(1) or 'Democratic' in sheet.row_values(1):
            cands = sheet.row_values(2)
            candidates = [c for c in cands[1:-1] if not c == ''][:len(offices)]
            raw_parties = sheet.row_values(1)[1:-1][:len(offices)]
            parties = []
            for p in raw_parties:
                if p != '':
                    previous = p
                    parties.append(p)
                elif p == '':
                    parties.append(previous)
                else:
                    break
        elif sheet.row_values(0)[1] != '':
            cands = []
            parties = []
            raw_cands = sheet.row_values(1)
            for cand in raw_cands:
                if "(" in cand:
                    parties.append(cand.split('(')[1].replace(')', '').strip())
                    cands.append(cand.split('(')[0].replace('  ',' ').strip())
                else:
                    parties.append(None)
                    cands.append(cand)
            candidates = [c.replace('\n', ' ') for c in cands[1:-1]][:len(offices)]
            parties = parties[1:-1][:len(offices)]
        return zip(candidates, offices, parties)

    def _build_contest_kwargs(self, office):
        # find a district number, if one exists (state house & senate only)
        if any(c.isdigit() for c in office):
            office = 'State ' + office.split(' ')[0]
            district = ''.join([c for c in office if c.isdigit()])
        else:
            district = None
        kwargs = {
            'office': office.strip(),
            'district': district,
        }
        return kwargs

    def _build_candidate_kwargs(self, candidate, party):
        # check if party is in name, extract if so
        full_name = candidate
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            'name_slug': slug,
            'party': party,
        }
        return kwargs

    def _base_kwargs(self, candidate, office, party):
        "Build base set of kwargs for RawResult"
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(office)
        candidate_kwargs = self._build_candidate_kwargs(candidate, party)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

    def _prep_precinct_result(self, precinct, candidate, office, party, votes):
        # each precinct has multiple candidate totals, plus write-ins, over and under votes
        print candidate
        kwargs = self._base_kwargs(candidate, office, party)
        if party:
            kwargs.update({'primary_party': party})
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            # In Wyoming, precincts are nested below counties.
            #
            # The mapping ocd_id will be for the precinct's county.
            # We'll save it as an expando property of the raw result because
            # we won't have an easy way of looking up the county in the 
            # transforms.
            'county_ocd_id': self.mapping['ocd_id'],
            'votes': votes,
            'vote_breakdowns': {},
        })
        return RawResult(**kwargs)

    def _votes(self, val):
        """
        Returns cleaned version of votes or 0 if it's a non-numeric value.
        """
        try:
            return int(float(val))
        except ValueError:
            # Count'y convert value from string   
            return None

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
