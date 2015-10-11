import re
import xlrd
import operator
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
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
        if election_id == 'wy-2008-11-25-special-general':
            loader = WYSpecialLoader2008()
        elif any(s in election_id for s in ['2006']):
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
        'President of the United States',
    ])

    office_segments = set([
        'United States',
        'House District',
        'Senate District',
        'Governor',
        'Secretary of',
        'State Auditor',
        'State Superintendent',
        'State Treasurer',
        'President of',
        'State Senate',
        'State House',
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
    Works for:
    2014 general & primary
    2012 general & primary
    2010 general & primary
    2008 general & primary
    2004 general & primary
    2002 general, primary & special general
    2000 general & primary
    """
    def load(self):
        year = int(re.search(r'\d{4}', self.election_id).group())
        xlsfile = xlrd.open_workbook(self._xls_file_path)
        if 'primary' in self._xls_file_path:
            primary = True
            if year == 2004:
                party = None # get party from individual sheets
            else:
                party = self._party_from_filepath(self._xls_file_path)
        else:
            primary = False
            party = None
        results = []

        sheets = self._get_sheets(xlsfile)
        for sheet in sheets:
            if year == 2004:
                if primary:
                    party = sheet.name.split()[1]
                candidates = self._build_candidates_2004(sheet, party)
            elif self.source == "20021126__wy__special__general__natrona__state_house__36__precinct.xls":
                candidates = self._build_candidates_2002_special(sheet)
            elif year < 2004:
                if primary:
                    if year == 2000:
                        party = self.source.split('__')[2].title()
                    else:
                        party = sheet.name.split()[1]
                if year == 2002:
                    candidates = self._build_candidates_2002(sheet, party)
                elif year == 2000:
                    candidates = self._build_candidates_2000(sheet, party, primary)
            else:
                candidates = self._build_candidates(sheet, party)

            for i in xrange(sheet.nrows):
                row = [r for r in sheet.row_values(i) if not r == '']
                # remove empty cells
                # Skip non-target offices
                if self._skip_row(row):
                    continue
                else:
                    precinct = str(row[0])
                    if self.source == '20021126__wy__special__general__natrona__state_house__36__precinct.xls':
                        votes = [v for v in row[1:] if not v == '']
                    elif len(candidates) == 1:
                        votes = [v for v in row[1:] if not v == '']
                    elif year == 2000 and primary is False:
                        precinct = row[0]
                        votes = [v for v in row[2:len(candidates)] if not v == precinct ]
                    elif year < 2006:
                        votes = [v for v in row[2:len(candidates)] if not v == '']
                    else:
                        votes = [v for v in row[1:len(candidates)] if not v == '']
                    grouped_results = zip(candidates, votes)
                    for (candidate, office, candidate_party), votes in grouped_results:
                        if not votes == '-':
                            results.append(self._prep_precinct_result(precinct, self.mapping['name'], candidate, office, candidate_party, votes))
            try:
                RawResult.objects.insert(results)
            except:
                print grouped_results
                raise

    def _get_sheets(self, xlsfile):
        if self.source == '20021126__wy__special__general__natrona__state_house__36__precinct.xls':
            sheets = [xlsfile.sheet_by_name('NA-HD36-Recount')]
        elif '2004' in self.source or '2002' in self.source:
            county = self.mapping['name']
            if 'general' in self.source:
                sheet_name = county + ' General'
                sheets = [xlsfile.sheet_by_name(sheet_name)]
            else:
                sheets = [s for s in xlsfile.sheets() if not 'Partisan' in s.name]
        elif '2000' in self.source:
            if 'general' in self.source:
                county = self.mapping['name']
                sheet_name = county + ' County'
                sheets = [xlsfile.sheet_by_name(sheet_name)]
            else:
                sheets = [xlsfile.sheets()[0]]
        else:
            try:
                sheets = [xlsfile.sheet_by_name('Sheet1')]
            except:
                sheets = [xlsfile.sheet_by_name('Party_PbP_Candidates_Summary')]
        return sheets

    def _party_from_filepath(self, path):
        return path.split("__")[2].title()

    def _skip_row(self, row):
        if row == []:
            return True
        if row[0] == 'Total':
            return True
        if row[0] == 'Official Totals':
            return True
        if isinstance(row[0], float):
            return False
        if row[0].strip() == '':
            return True
        if row[0].startswith("* The State Canvassing Board"):
            return True
        if row[0].replace('\n',' ').strip() in self.target_offices:
            return True
        # if the contents of the second cell is not a float, skip that row
        try:
            float(row[1])
            return False
        except ValueError:
            return True
        except IndexError:
            return True

    def _build_offices_2004(self, sheet):
        a = sheet.row_values(0)[2:]
        b = sheet.row_values(1)[2:]
        raw_offices = [" ".join(x) for x in zip(a,b)]
        office_labels = [x for x in raw_offices if " ".join(x.split()[0:2]).strip() in self.office_segments]
        office_labels = list(set(office_labels))
        offices = []
        for o in raw_offices:
            if o in office_labels:
                office = o.replace('\n', ' ').replace('  ',' ').strip()
                previous = office
                offices.append(office)
            elif o.strip() == '':
                offices.append(previous)
            else:
                continue
        return offices

    def _build_offices_2002(self, sheet):
        a = [x.strip() for x in sheet.row_values(0)[2:]]
        b = [x.strip() for x in sheet.row_values(1)[2:]]
        c = [x.strip() for x in sheet.row_values(2)[2:]]
        raw_offices = [" ".join(x) for x in zip(a,b,c)]
        # some office names are shifted to the right
        raw_offices[2] = raw_offices[3]
        raw_offices[3] = ''
        office_labels = [x for x in raw_offices if " ".join(x.split()[0:2]).strip() in self.office_segments]
        office_labels = list(set(office_labels))
        offices = []
        for o in raw_offices:
            if o in office_labels:
                office = o.replace('\n', ' ').replace('  ',' ').strip()
                previous = office
                offices.append(office)
            elif o.strip() == '':
                offices.append(previous)
            else:
                continue
        return offices

    def _build_offices_2002_special(self, sheet):
        offices = []
        offices.append(" ".join([sheet.row_values(0)[1], sheet.row_values(1)[1]]))
        offices.append(" ".join([sheet.row_values(0)[1], sheet.row_values(1)[1]]))
        return offices

    def _build_offices_2000(self, sheet, party):
        if 'primary' in self.source:
            # always skip any columns that don't have a cand
            cand_cols = [sheet.row_values(3).index(x) for x in sheet.row_values(3)[2:] if not x == '']
            a = operator.itemgetter(*cand_cols)(sheet.row_values(0))
            b = operator.itemgetter(*cand_cols)(sheet.row_values(1))
        else:
            cand_cols = [sheet.row_values(4).index(x) for x in sheet.row_values(4)[2:] if not x == '']
            a = operator.itemgetter(*cand_cols)(sheet.row_values(0))
            b = operator.itemgetter(*cand_cols)(sheet.row_values(1))
        raw_offices = [" ".join(x) for x in zip(a,b)]
        office_labels = [x for x in raw_offices if " ".join(x.split()[0:2]).strip() in self.office_segments]
        office_labels = list(set(office_labels))
        offices = []
        for o in raw_offices:
            if o in office_labels:
                office = o.replace('\n', ' ').replace('  ',' ').strip()
                previous = office
                offices.append(office)
            elif o.strip() == '':
                offices.append(previous)
            else:
                continue
        return offices

    def _build_offices(self, sheet):
        if sheet.row_values(0)[1] != '':
            raw_offices = sheet.row_values(0)[1:]
        else:
            raw_offices = sheet.row_values(1)[1:]
        if raw_offices[0] == '' or raw_offices[0] == 'Total':
            del raw_offices[0]
        office_labels = [x for x in raw_offices if " ".join(x.split()[0:2]).strip() in self.office_segments]
        office_labels = list(set(office_labels)) # unique it
        offices = []
        for o in raw_offices:
            if o in office_labels:
                previous = o.replace('\n', ' ')
                offices.append(o.replace('\n', ' '))
            elif o == '':
                offices.append(previous)
            else:
                break
        return offices

    def _build_candidates_2004(self, sheet, party):
        offices = self._build_offices_2004(sheet)
        a = sheet.row_values(3)[2:]
        b = sheet.row_values(4)[2:]
        raw_cands = [" ".join(x) for x in zip(a,b)][:len(offices)]
        candidates = []
        parties = []
        for cand in raw_cands:
            if "(" in cand:
                parties.append(cand.split('(')[1].replace(')', '').strip())
                candidates.append(cand.split('(')[0].replace('  ',' ').strip())
            else:
                parties.append(party)
                candidates.append(cand)
        return zip(candidates, offices, parties)

    def _build_candidates_2002_special(self, sheet):
        offices = self._build_offices_2002_special(sheet)
        raw_cands = sheet.row_values(3)[1:]
        candidates = []
        parties = []
        for cand in raw_cands:
            parties.append(cand.split(' - ')[1])
            candidates.append(cand.split(' - ')[0])
        return zip(candidates, offices, parties)

    def _build_candidates_2002(self, sheet, party):
        offices = self._build_offices_2002(sheet)
        raw_cands = [x for x in sheet.row_values(3)[2:] if x.strip() not in ('Yes', 'No', 'For', 'Against', '')]
        offices = offices[:len(raw_cands)]
        candidates = []
        parties = []
        for cand in raw_cands:
            if party:
                parties.append(party)
                candidates.append(cand)
            else:
                parties.append(cand.split('-')[1].strip())
                candidates.append(cand.split('-')[0].strip())
        return zip(candidates, offices, parties)

    def _build_candidates_2000(self, sheet, party, primary):
        offices = self._build_offices_2000(sheet, party)
        if primary:
            raw_cands = [x for x in sheet.row_values(3)[2:] if x.strip() not in ('Yes', 'No', 'For', 'Against', '')][:len(offices)]
        else:
            raw_cands = [x for x in sheet.row_values(4)[2:] if x.strip() not in ('Yes', 'No', 'For', 'Against', '')][:len(offices)]
        candidates = []
        parties = []
        for cand in raw_cands:
            if "(" in cand:
                parties.append(cand.split('(')[1].replace(')', '').strip())
                candidates.append(cand.split('(')[0].replace('  ',' ').strip())
            else:
                parties.append(party)
                candidates.append(cand)
        return zip(candidates, offices, parties)

    def _build_candidates(self, sheet, party):
        # map candidates to offices so we can lookup up one and get the other
        # for our purposes, candidates include totals for write-ins, over and under votes
        # TODO: filter out write-ins, over and under votes
        offices = self._build_offices(sheet)
        if 'Republican' in sheet.row_values(1) or 'Democratic' in sheet.row_values(1):
            cands = sheet.row_values(2)[1:-1]
            # find indexes of empty cand cells and remove them from offices
            empty_cells = [i for i , c in enumerate(cands) if c == '']
            offices = [i for j, i in enumerate(offices) if j not in empty_cells]
            candidates = [c for c in cands if not c == ''][:len(offices)]
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
        else:
            cands = []
            parties = []
            raw_cands = [c for c in sheet.row_values(2) if not c == '']
            for cand in raw_cands:
                if " - " in cand:
                    parties.append(cand.split(' - ')[1].strip())
                    cands.append(cand.split(' - ')[0].strip())
                else:
                    parties.append(party)
                    cands.append(cand)
            candidates = [c.replace('\n', ' ') for c in cands[:len(offices)]]
            if parties[0] == '':
                parties = parties[1:]
        return zip(candidates, offices, parties)

    def _build_contest_kwargs(self, office):
        # find a district number, if one exists (state house & senate only)
        if any(c.isdigit() for c in office):
            district = ''.join([c for c in office if c.isdigit()])
            office = 'State ' + office.split(' ')[0]
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

    def _prep_precinct_result(self, precinct, county, candidate, office, party, votes):
        # each precinct has multiple candidate totals, plus write-ins, over and under votes
        kwargs = self._base_kwargs(candidate, office, party)
        if party:
            kwargs.update({'primary_party': party})
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            'parent_jurisdiction': county,
            'ocd_id': "{}/precinct:{}".format(self.mapping['ocd_id'],
                ocd_type_id(precinct)),
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
    Loads Wyoming results for 2006 and for 2002 special election.

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
                    jurisdiction = row['precinct'].strip()
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': jurisdiction,
                        'ocd_id': "{}/precinct:{}".format(self.mapping['ocd_id'],
                            ocd_type_id(jurisdiction)),
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

class WYSpecialLoader2008(WYBaseLoader):
    """
    Loads Wyoming results for 2008 special elections.

    Format:

    Wyoming has PDF files that have been converted to CSV files with office names that correspond
    to those used for elections in 2006 and for special elections.
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
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames=headers, encoding='latin-1')
            for row in reader:
                if row['votes'] == 'votes':
                    continue
                if row['county'].strip() == '':
                    total_votes = int(row['votes'].strip())
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    rr_kwargs.update({
                        'party': row['party'].strip(),
                        'jurisdiction': row['county'].strip(),
                        'votes': int(row['votes'].strip()),
                        'winner': row['winner'].strip(),
                        'ocd_id': self.mapping['ocd_id'],
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
