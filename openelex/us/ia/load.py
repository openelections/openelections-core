import logging
import re

import unicodecsv
import xlrd

from openelex.base.load import BaseLoader
from openelex.lib.text import ocd_type_id
from openelex.models import RawResult
from openelex.us.ia.datasource import Datasource


class LoadResults(object):
    """
    Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    # TODO: Don't skip these.  They're being skipped because the PDF results
    # haven't been converted yet in favor of higher-value files
    SKIP_FILES = [
        # These files originate as PDFs and haven't been extracted yet
        '20010612__ia__special__general__state_house__85__county.csv',
        '20010612__ia__special__general__state_senate__43__county.csv',
        '20011106__ia__special__general__state_house__82__county.csv',
        '20020122__ia__special__general__state_house__28__county.csv',
        '20020219__ia__special__general__state_senate__39__county.csv',
        '20021105__ia__general__precinct.csv',
        '20030114__ia__special__general__state_senate__26__county.csv',
        '20030211__ia__special__general__state_house__62__county.csv',
        '20030805__ia__special__general__state_house__100__county.csv',
        '20030826__ia__special__general__state_house__30__county.csv',
        '20040203__ia__special__general__state_senate__30__county.csv',
        '20091124__ia__special__general__state_house__33__precinct.csv',
        '20100608__ia__primary__county.csv',
        # The following files have significantly different structure than the
        # rest of the 2010 general election precinct-level files and from
        # each other.  Skip them for now.
        #
        # TODO: Write separate loader classes for these
        '20101102__ia__general__audubon__precinct.xls',
        '20101102__ia__general__clinton__precinct.xls',
        '20101102__ia__general__grundy__precinct.xls',
        '20101102__ia__general__henry__precinct.xls',
        '20101102__ia__general__johnson__precinct.xls',
        '20101102__ia__general__louisa__precinct.xls',
        '20101102__ia__general__poweshiek__precinct.xls',
    ]

    def run(self, mapping):
        loader = self._get_loader(mapping)
        loader.run(mapping)

    def _get_loader(self, mapping):
        election_id = mapping['election']
        election_year = int(election_id[3:7])
        generated_filename = mapping['generated_filename']

        if generated_filename in self.SKIP_FILES:
            return SkipLoader()
        elif ('precinct' in generated_filename and
              generated_filename.endswith('xls') and
              election_year < 2010):
            return ExcelPrecinctPre2010ResultLoader()
        elif ('precinct' in generated_filename and
              generated_filename.endswith('xls') and
              election_year == 2010 and
              'primary' in election_id):
            return ExcelPrecinct2010PrimaryResultLoader()
        elif ('precinct' in generated_filename and
              generated_filename.endswith('xls') and
              election_year == 2010 and
              'general' in election_id):
            return ExcelPrecinct2010GeneralResultLoader()
        elif (election_year == 2012 and generated_filename.endswith('xls')):
            return ExcelPrecinct2012ResultLoader()
        elif (election_year == 2014 and generated_filename.endswith('xlsx')
              and 'primary' in election_id):
            return ExcelPrecinct2014ResultLoader()
        elif 'pre_processed_url' in mapping:
            return PreprocessedResultsLoader()

        raise ValueError("Could not get appropriate loader for file {}".format(
            generated_filename))


class SkipLoader(object):
    def run(self, mapping):
        logging.warn("Skipping file {}".format(mapping['generated_filename']))


class PreprocessedResultsLoader(BaseLoader):
    """
    Load CSV results that were converted from the original PDFs

    These results are fetched from the openelections-data-ia repo.

    A few caveats about the data:

    * The fields are generally consistent, but there may be some missing/added
      fields between elections.
    * Many results files contain aggregate vote counts for "pseudo-candidates"
      such as "Write-In", or "Totals".  These values appear in the
      ``candidate`` column.
    * Many results files contain racewide totals.  These will have a value of
      something like "Totals" in the jurisdiction field.

    """
    datasource = Datasource()

    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue

                if self._is_racewide_total(row):
                    # Regardless of the reporting level of the file, rows with
                    # a jurisdiction of "Totals" should be interpretted as a
                    # racewide result
                    results.append(self._prep_racewide_result(row))
                elif 'precinct' in self.source:
                    # TODO: Handle absentee and provisional votes in these
                    # results.  These are in:
                    # 20001107__ia__general__precinct.csv
                    # 20041102__ia__general__precinct.csv
                    county = None
                    if self.mapping['name'] != 'Iowa':
                        county = self.mapping['name']
                    results.append(self._prep_precinct_result(row, county=county))
                elif 'county' in self.source:
                    # TODO: Handle over and under votes in these results.
                    # These are in:
                    # 20061107__ia__general__county.csv
                    # 20061107__ia__general__governor__county.csv
                    # 20080603__ia__primary__county.csv
                    # 20081104__ia__general__county.csv
                    # 20090901__ia__special__general__state_house__90__county.csv
                    # 20091124__ia__special__general__state_house__33__county.csv
                    # 20101102__ia__general__county.csv
                    # 20101102__ia__general__governor__county.csv
                    # 20101102__ia__general__state_house.csv
                    # 20101102__ia__general__state_senate.csv
                    # 20101102__ia__general__us_house_of_representatives__county.csv
                    # 20101102__ia__general__us_senate__county.csv
                    results.append(self._prep_county_result(row))
                else:
                    raise Exception("Unknown reporting level for result")

            RawResult.objects.insert(results)

    def _skip_row(self, row):
        if (self.mapping['election'] == "ia-2004-11-02-general" and
                'precinct' in self.source and 'county' in row and
                row['county'].endswith(" Total")):
            # Skip county-level totals in 2004 general election precinct-level
            # results
            return True

        return False

    def _is_racewide_total(self, row):
        try:
            return row['jurisdiction'].strip().upper() in ["TOTALS", "TOTAL"]
        except KeyError:
            # No jurisdiction column means racewide result
            return True

    def _build_contest_kwargs(self, row, primary_type):
        kwargs = {
            'office': row['office'].strip(),
        }
        try:
            kwargs['district'] = row['district'].strip()
        except KeyError:
            # Some office-specific results files, such as the presidential
            # race results in 2000 don't have a district field
            pass
        # Add party if it's a primary
        if primary_type == 'closed':
            kwargs['primary_party'] = row['party'].strip()
        return kwargs

    def _build_candidate_kwargs(self, row):
        kwargs = {
            'party': row['party'].strip(),
        }

        # These are all PDF-based results, designed for visual consumption.
        # As such, treat all names as full names since they aren't explicitely
        # separated out by first or last name
        try:
            kwargs['full_name'] = row['candidate'].strip()

            # As far as I can tell, there aren't any fields identifying a
            # candidate as a write-in candidate.  However, there are
            # pseudo-candidate tallies of the combined votes for all write-in
            # candidates.  I guess this should set the write_in flag on the
            # model
            if re.search(r'Write[- ]In', row['candidate']):
                kwargs['write_in']  = True
        except KeyError:
            # Some results files, such as the 2000-11-07 general election's
            # precinct-level results don't have candidates, only the party
            # of the candidate
            pass

        return kwargs

    def _base_kwargs(self, row):
        "Build base set of kwargs for RawResult"
        kwargs = self._build_common_election_kwargs()
        # Sanity check that the primary_type field has been set on all records
        # that we're getting from the elections API
        assert (kwargs['election_type'] is not 'primary' or
                kwargs['primary_type'] == 'closed')
        contest_kwargs = self._build_contest_kwargs(row, kwargs['primary_type'])
        candidate_kwargs = self._build_candidate_kwargs(row)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        try:
            kwargs['jurisdiction'] = row['jurisdiction'].strip()

        except KeyError:
            # Some statewide results, often special elections, don't have
            # jurisdiction fields
            kwargs['jurisdiction'] = "Iowa"
        kwargs['votes'] = self._votes(row['votes'])

        # Handle special fields
        try:
            kwargs['winner'] = row['winner'].upper() == "TRUE"
        except KeyError:
            # This file doesn't have a winner field. It's ok.
            pass

        return kwargs

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs['reporting_level'] = 'county'
        # Use kwargs['jurisdiction'] instead of row['jurisdiction'] because
        # the value in the kwargs dict will already have been cleaned to
        # enable proper lookup.
        try:
            kwargs['ocd_id'] = self._lookup_county_ocd_id(kwargs['jurisdiction'])
        except KeyError:
            # Some county names are mispelled, such as "Van Buren" County
            # being mispelled "VanBuren" in the 2000-11-07 general election
            # county-level results file.
            pass

        return RawResult(**kwargs)

    def _prep_precinct_result(self, row, county=None):
        kwargs = self._base_kwargs(row)
        if county is None:
            county = row['county']
        county_ocd_id = self._lookup_county_ocd_id(county)
        ocd_id = "{}/precinct:{}".format(county_ocd_id,
            ocd_type_id(row['jurisdiction']))
        kwargs.update({
            'reporting_level': 'precinct',
            'ocd_id': ocd_id,
        })
        return RawResult(**kwargs)

    def _prep_racewide_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update({
          'reporting_level': 'state',
          'jurisdiction': "Iowa",
          'ocd_id': self.mapping['ocd_id'],
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
            # Can't convert value from string
            return 0
        pass

    def _lookup_county_ocd_id(self, county):
        """Retrieve the ocd_id for a given county"""
        try:
            return self._county_ocd_id_cache[county]
        except AttributeError:
            self._county_ocd_id_cache = {}
        except KeyError:
            pass

        # No cached value, look it up from the jurisdiction mappings
        for mapping in self.datasource._counties():
            if mapping['name'].lower() == county.lower():
                self._county_ocd_id_cache[county] = mapping['ocd_id']
                return self._county_ocd_id_cache[county]

        raise KeyError("No county matching '{}' found".format(county))


class ExcelPrecinctResultLoader(BaseLoader):
    """Base class for parsing precinct-level results in Excel format"""
    datasource = Datasource()

    def load(self):
        RawResult.objects.insert(self._results(self.mapping))

    def _results(self, mapping):
        return []

    def _rows(self, sheet=None):
        if sheet is None:
            sheet = self._get_sheet()

        for row_index in xrange(sheet.nrows):
            yield [r for r in sheet.row_values(row_index)]

    def _get_sheet(self, workbook=None, sheet_index=0):
        """Return the sheet of interest from the workbook"""
        if workbook is None:
            workbook = self._get_workbook()

        return workbook.sheets()[sheet_index]

    def _get_workbook(self):
        return xlrd.open_workbook(self._xls_file_path)


class ExcelPrecinctPre2010ResultLoader(ExcelPrecinctResultLoader):
    """
    Parse precinct-level results in Excel format in the structure used
    through 2008
    """
    _office_re = re.compile(r'(?P<office>Attorney General|Auditor of State|'
        'Governor and Lieutenant Governor|'
        'Secretary of State|Secretary of Agriculture|'
        'Treasurer of State|State Representative|State Senator|'
        'United States Representative|United States Senator|'
        'President/Vice President)'
        '(\s+District\s+(?P<district>\d+)|)')

    HEADER_CELLS = [
        'Race',
        'Candidates',
        'Precincts',
        'Total',
    ]
    """
    Some result files, such as the 2008 precinct-level results have these
    values in the second through fifth rows.  They describe the order of
    the rows in the following sections.  We should ignore them when
    processing the results.
    """

    COUNTY_JURISDICTIONS = (
        "Totals",
        "ABSENTEE PRECINCT",
        "PROVISIONAL PRECINCT",
    )
    """
    Values in the jurisdiction column that indicate that the votes are a
    total for all the county's precincts rather than a specific precinct.
    """

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        office = None
        district = None
        candidates_next = False
        base_kwargs = self._build_common_election_kwargs()
        row_num = -1

        for row in self._rows():
            row_num += 1

            # We'll inspect the first two cells to figure out
            # the function of the row
            try:
                cell0 = row[0].strip()
            except AttributeError:
                # No strip() method, probably because it's not a string.
                # We know that this should always be interpretted as a
                # string, so we can go ahead and convert it.
                cell0 = str(int(row[0]))

            try:
                cell1 = row[1].strip()
            except AttributeError:
                # No strip() method, probably because it's not a string
                cell1 = row[1]

            if candidates_next:
                candidates_next = False
                # There will be some empty candidate columns, but
                # keep them to make it easier to iterate through
                # the results
                candidates = [col.strip() for col in row[1:]]
                continue

            if cell0 == '' and cell1 == '':
                # Empty row, skip it
                continue

            if cell0 and cell1 == '':
                if row_num == 0:
                    # County header, skip it
                    continue

                if cell0 in self.HEADER_CELLS:
                    continue

                office, district = self._parse_office(cell0)
                if office is None:
                    logging.info("Skipping office {}".format(cell0))

                candidates_next = True
                candidates = None
                common_kwargs = {
                    'office': office,
                    'district': district,
                }
                common_kwargs.update(base_kwargs)
                continue

            if cell0 != " "and cell1 != "":
                # Result row
                row_results = self._parse_result_row(row, candidates, county,
                    county_ocd_id, **common_kwargs)
                results.extend(row_results)

        return results

    def _parse_office(self, s):
        """
        Parse offices of interest

        Args:
            s (string): String representing a cell in the raw data table

        Returns:
            Tuple of strings representing office and district.  Returns None
            for office if the string doesn't match an office of interest.
            Returns None for district if the office doesn't have a district.

        """
        m = self._office_re.match(s)
        if m is None:
            return None, None

        return m.group('office'), m.group('district')

    def _parse_result_row(self, row, candidates, county, county_ocd_id,
            **common_kwargs):
        """
        Create result dictionaries from a row of results for a precinct

        Args:
            row (list): Data from a row in the spreadsheet.  The first column
                should be the precinct name.  All other columns are vote
                values
            candidates (list): List of candidate full names
            county (string): County name
            count_ocd_id (string): OCD ID for the county

        Returns:
            A list of RawResult models with each dictionary representing the
            votes for a candidate (or pseudo-candidate) for the reporting
            level.

        """
        results = []
        assert len(candidates) == len(row) - 1
        i = 1
        raw_jurisdiction = row[0]
        if raw_jurisdiction in self.COUNTY_JURISDICTIONS:
            # Total of all precincts is a county-level result
            jurisdiction = county
            ocd_id = county_ocd_id
            reporting_level = 'county'
        else:
            jurisdiction = raw_jurisdiction
            try:
                ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)
            except TypeError:
                # Jurisdiction is interpretted as a number, not a string
                ocd_id = county_ocd_id + '/' + ocd_type_id(str(int(jurisdiction)))

            reporting_level = 'precinct'

        for candidate in candidates:
            if candidate != "":
                results.append(RawResult(
                    full_name=candidate,
                    votes=row[i],
                    votes_type=self._votes_type(candidate, raw_jurisdiction),
                    jurisdiction=jurisdiction,
                    ocd_id=ocd_id,
                    reporting_level=reporting_level,
                    **common_kwargs
                ))

            i += 1

        return results

    @classmethod
    def _votes_type(cls, candidate, jurisdiction):
        # TODO: Need to figure out how to handle results that are
        # over/under votes for absentee ballots
        # See
        # https://github.com/openelections/core/issues/211#issuecomment-57338832
        if "absentee" in jurisdiction.lower():
            return 'absentee'

        if "provisional" in jurisdiction.lower():
            return 'provisional'

        if candidate == "OverVote":
            return 'over'

        if candidate == "UnderVote":
            return 'under'

        return ''


class ExcelPrecinct2010PrimaryResultLoader(ExcelPrecinctResultLoader):
    """
    Parse precinct-level results in Excel format in the structure used
    including and after 2010
    """
    _office_re = re.compile(r'(?P<office>U.S. SENATOR|U.S. REPRESENTATIVE|'
            'GOVERNOR|SECRETARY OF STATE|AUDITOR OF STATE|TREASURER OF STATE|'
            'SECRETARY OF AGRICULTURE|ATTORNEY GENERAL|'
            'STATE SENATOR|STATE REPRESENTATIVE)'
            '( DISTRICT (?P<district>\d+)|)'
            '( - (?P<party>.+) PARTY|)')

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()

        for row in self._rows():
            cell0 = row[0].strip()

            if cell0 == "Race":
                candidates = self._parse_candidates(row)
                office = None
                district = None
                party = None
                continue

            # This is a results row

            if office is False:
                # This is an office we don't care about
                continue

            if office is None:
                # We haven't parsed the office, district or party
                # for this office yet
                office, district, party = self._parse_office_party(cell0)
                common_kwargs = {
                  'office': office,
                  'district': district,
                  'party': party,
                }
                if 'primary' in mapping['election']:
                    common_kwargs['primary_party'] = party
                common_kwargs.update(base_kwargs)

            row_results = self._parse_result_row(row, candidates, county,
                county_ocd_id, **common_kwargs)

            results.extend(row_results)

        return results

    def _parse_office_party(self, val):
        office = False
        district = None
        party = None
        m = self._office_re.match(val)
        if m:
            office = m.group('office')
            district = m.group('district')
            party = m.group('party')

        return office, district, party

    def _parse_result_row(self, row, candidates, county, county_ocd_id,
            **common_kwargs):
        results = []
        i = 0

        raw_jurisdiction = row[2]
        if row[0] == "Grand Totals" or raw_jurisdiction == "ABSENTEE":
            # Total of all precincts is a county-level result
            jurisdiction = county
            reporting_level = 'county'
        else:
            jurisdiction = raw_jurisdiction
            reporting_level = 'precinct'

        # Iterate through the vote columns.  Skip the first 3 colums (Race,
        # County, Precinct) and the last one (Final Data?)
        for col in row[3:3 + len(candidates)]:
            candidate = candidates[i]
            results.append(RawResult(
                jurisdiction=jurisdiction,
                reporting_level=reporting_level,
                full_name=candidate,
                votes=col,
                votes_type=self._votes_type(candidate, raw_jurisdiction),
                **common_kwargs
            ))
            i += 1

        return results

    def _parse_candidates(self, row):
        candidates = []
        for col in row[3:]:
            col_clean = col.strip()
            if col_clean == "Final Data?":
                return candidates

            candidates.append(col_clean)

        raise AssertionError("Unexpected candidate columns")

    @classmethod
    def _votes_type(cls, candidate, jurisdiction):
        # TODO: Need to figure out how to handle results that are
        # over/under votes for absentee ballots
        # See
        # https://github.com/openelections/core/issues/211#issuecomment-57338832
        if "absentee" in jurisdiction.lower():
            return 'absentee'

        if candidate == "Over Votes":
            return 'over'

        if candidate == "Under Votes":
            return 'under'

        return ''


class ExcelPrecinct2010GeneralResultLoader(ExcelPrecinctResultLoader):
    _county_re = re.compile(r'(?P<county>[A-Za-z ]+) County')
    _office_re = re.compile(r'(?P<office>U\.{0,1}S\.{0,1} Senator|'
        'U\.{0,1}S\.{0,1} Rep(resentative|)|'
        'Governor/Lt\.{0,1} Governor|Secretary of State|Auditor of State|'
        'Treasurer of State|Secretary of Agriculture|Attorney General|'
        'State Senator|State Rep)'
        '(\s+Dist (?P<district>\d+)|)', re.IGNORECASE)

    _skip_candidates = (
        'Number of Precincts for Race',
        'Number of Precincts Reporting',
        'Registered Voters',
        'Times Counted',
        'Times Blank Voted',
        'REGISTERED VOTERS - TOTAL',
        'BALLOTS CAST - TOTAL',
    )

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        # Some sheets have annoying leading hidden columns.
        # Detect the start column
        sheet = self._get_sheet()
        jurisdiction_offset = self._col_offset(sheet)
        if county == "Emmet":
            race_offset = jurisdiction_offset + 1
        else:
            race_offset = jurisdiction_offset

        for row in self._rows(sheet):
            cell0 = self._get_first_cell(row, jurisdiction_offset)

            if self._county_re.match(cell0):
                continue

            if cell0 == "Precinct":
                continue

            # This is a results row
            row_results = self._parse_result_row(row, county, county_ocd_id,
                jurisdiction_offset, race_offset, **base_kwargs)

            if row_results:
                results.extend(row_results)

        return results

    def _col_offset(self, sheet):
        """
        Detect hidden leading columns

        This is the case for Emmet and Van Buren counties
        """
        row = sheet.row_values(0)
        for offset in range(len(row)):
            if row[offset] != '':
                return offset

        raise AssertionError("No nonempty columns found in first row")

    def _parse_result_row(self, row, county, county_ocd_id, jurisdiction_offset=0,
            race_offset=0,
            **common_kwargs):
        results = []

        vote_breakdowns_in_cols = self._vote_breakdowns_in_cols(row,
                jurisdiction_offset)

        raw_office = self._get_office(row, race_offset)

        m = self._office_re.match(raw_office)
        if not m:
            logging.warn("Skipping office '{}'".format(raw_office))
            return None

        candidate = self._get_candidate(row, race_offset)

        if candidate in self._skip_candidates:
            logging.warn("Skipping candidate '{}'".format(candidate))
            return None

        office = m.group('office')
        district = m.group('district')
        jurisdiction = self._get_jurisdiction(row, jurisdiction_offset)

        if jurisdiction == "Election Total":
            reporting_level = 'county'
            ocd_id = county_ocd_id
            jurisdiction = county
        else:
            reporting_level = 'precinct'
            ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)

        # These fields will be the same regardless of the layout of the results
        result_kwargs = dict(
            full_name=candidate,
            office=office,
            district=district,
            reporting_level=reporting_level,
            jurisdiction=jurisdiction,
            ocd_id=ocd_id,
            **common_kwargs
        )

        if not vote_breakdowns_in_cols:
            # Separate row for each vote breakdown type
            votes = self._get_votes(row, race_offset)
            votes_type = self._votes_type(self._get_votes_type(row, race_offset))
        else:
            # Each vote breakdown type has its own column in the same row
            votes = self._get_total_votes(row, race_offset)
            votes_type = ''

        results.append(RawResult(
          votes=votes,
          votes_type=votes_type,
          **result_kwargs
        ))

        if vote_breakdowns_in_cols:
            # When each vote breakdown type has its own column in the same row
            # add the election day (polling) and absentee results as well
            results.append(RawResult(
              votes=self._get_polling_votes(row, race_offset),
              votes_type='election_day',
              **result_kwargs
            ))

            results.append(RawResult(
              votes=self._get_absentee_votes(row, race_offset),
              votes_type='absentee',
              **result_kwargs
            ))

        return results

    def _vote_breakdowns_in_cols(self, row, offset=0):
        """
        Detect whether vote breakdowns are in separate rows or columns

        There are two different, but similar layouts for results files.
        One puts total, election day and absentee votes in separate rows.
        Another puts the total, election day and absentee votes
        in separate columns.  An example of this is
        20101102__ia__general__wapello__precinct.xls

        """
        return len(row) > (5 + offset)

    def _get_first_cell(self, row, offset=0):
        return row[0 + offset].strip()

    def _get_office(self, row, offset=0):
        return row[1 + offset].strip()

    def _get_candidate(self, row, offset=0):
        return row[2 + offset].strip()

    def _get_jurisdiction(self, row, offset=0):
        return row[0 + offset].strip()

    def _get_votes(self, row, offset=0):
        return row[4 + offset]

    def _get_votes_type(self, row, offset=0):
        return row[3 + offset].strip()

    def _get_total_votes(self, row, offset=0):
        return row[5 + offset]

    def _get_polling_votes(self, row, offset=0):
        return row[3 + offset]

    def _get_absentee_votes(self, row, offset=0):
        return row[4 + offset]

    def _votes_type(self, val):
        if val in ("Polling", "Absentee"):
            return val.lower()

        # Otherwise, assume normal vote totals
        return ""

class ExcelPrecinct2012ResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2012 primary and general election precinct-level results

    Some notes about these files:

    In the primary, the primary party is indicated as "D" or "R" and is
    included in the same cell as the office, e.g.
    "U.S. House of Representatives District 3 - D"

    The structure of a single contest's record group looks like this:

    Office
    Candidates
    "Precinct" heading row
    Absentee pseudo-precinct result row
    Per-precinct results rows
    County total result row

    """
    offices = [
        "U.S. House of Representatives",
        "State Senator",
        "State Representative",
        "President/Vice President",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s+District (?P<district>\d+)){{0,1}}'
        '(?:\s+-\s+(?P<party>D|R)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE)

    # HACK: List of Polk County precincts to reflect header
    POLK_PRECINCTS = [
        'ABSENTEE 1',
        'ALLEMAN-1',
        'ANKENY-1',
        'ANKENY-10',
        'ANKENY-11',
        'ANKENY-12',
        'ANKENY-13',
        'ANKENY-2',
        'ANKENY-3',
        'ANKENY-4',
        'ANKENY-5',
        'DOUGLAS-1',
        'SPECIAL',
        'Total',
    ]

    def _results(self, mapping):
        results = []

        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        in_contest_results = False
        base_kwargs = self._build_common_election_kwargs()
        sheet = self._get_sheet()

        for row in self._rows(sheet):
            if self._empty_row(row) or self._page_header_row(row):
                continue

            if row[0] == "Precinct" and 'primary' in mapping['election']:
                # Primary results have a "Precinct" column heading row.
                # Skip it.
                continue

            office, district, primary_party = self._parse_office_row(row)
            if office:
                in_contest_results = True
                common_kwargs = {
                    'office': office,
                    'district': district,
                    'reporting_level': None,
                }
                if primary_party:
                    common_kwargs['primary_party'] = primary_party
                common_kwargs.update(base_kwargs)
                candidates = None
                continue

            if candidates is None:
                new_candidates = self._parse_candidates_row(row)
                if new_candidates:
                    candidates = new_candidates + [('Total', None)]
                    continue

            if not in_contest_results:
                continue

            # A bit of acrobatics because the general election results
            # have separate rows for election day, absentee and
            # total results
            jurisdiction, level = self._parse_jurisdiction(row,
                common_kwargs['reporting_level'])
            if jurisdiction != '':
                common_kwargs['jurisdiction'] = jurisdiction
            common_kwargs['reporting_level'] = level
            if common_kwargs['reporting_level'] == 'county':
                common_kwargs['jurisdiction'] = county

            row_results = self._parse_result_row(row, candidates, county,
                county_ocd_id, **common_kwargs)

            if row_results:
                results.extend(row_results)

            if self._is_last_contest_result(row, common_kwargs['reporting_level']):
                in_contest_results = False

        return results

    def _page_header_row(self, row):
        if ("ELECTION CANVASS SUMMARY" in row[4] or
            "ELECTION CANVASS SUMMARY" in row[3]):
            return True
        else:
            return False

    def _empty_row(self, row):
        for col in row:
            if col != '':
                return False

        return True

    def _parse_office_row(self, row):
        """
        Parse the office, district and primary party from a spreadsheet row

        Args:
            row (list): Columns in spreadsheet row

        Returns:
            Tuple representing office, district and primary party. If the row
            does not match an office, the office tuple component will be None.
            If there is no district corresponding to the office, or no primary
            party, those components will be None.

        """
        office = None
        district = None
        primary_party = None
        m = None
        # Get first nonempty column in row
        try:
            col = next(c for c in row if c != '')
        except StopIteration:
            # No nonempty columns, return all None
            return office, district, primary_party

        m = self._office_re.search(col)
        if not m:
            # First nonempty column doesn't match an office of interest.
            # Return all None
            return office, district, primary_party

        office = m.group('office')
        district = m.group('district')
        primary_party = m.group('party')

        return office, district, primary_party

    def _parse_candidates_row(self, row):
        """
        Parse candidates and parties from spreadsheet row

        Args:
            row (list): Columns in spreadsheet row

        Returns:
            List of tuples representing candidate name, party pairs.
            None if the row doesn't match a list of canidates.
            If there is no party information in this row, which is the case
            in a primary election, the party member of the tuple will be None.

        """
        if row[0] not in ('', "Precinct"):
            return None

        candidates = []

        for col in row[1:]:
            col = col.strip()
            if col == '':
                continue

            bits = col.split('\n')
            candidate = bits[0]
            party = None
            if len(bits) > 1:
                party = bits[1]

            candidates.append((candidate, party))

        if len(candidates):
            return candidates
        else:
            return None

    def _parse_jurisdiction(self, row, prev_reporting_level):
        jurisdiction = row[0].strip()
        cell2 = row[2].strip()
        reporting_level = 'precinct'

        if (jurisdiction in ("Absentee", "Total") or
            (jurisdiction == "" and cell2 == "Election Day") or
            (prev_reporting_level == 'county' and cell2 in ("Absentee", "Total"))):
            reporting_level = 'county'

        return jurisdiction, reporting_level

    def _parse_result_row(self, row, candidates, county, county_ocd_id, **common_kwargs):
        results = []
        candidate_index = 0
        fixed_row = self._fix_row(row)

        for col in fixed_row[3:]:
            if col != '':
                party = candidates[candidate_index][1]

                if common_kwargs['reporting_level'] == 'county':
                    ocd_id = county_ocd_id
                else:
                    ocd_id = county_ocd_id + '/' + ocd_type_id(common_kwargs['jurisdiction'])

                if (party is None and 'primary_party' in common_kwargs
                        and common_kwargs['primary_party']):
                    party = common_kwargs['primary_party']

                results.append(RawResult(
                    full_name=candidates[candidate_index][0],
                    votes=col,
                    votes_type=self._parse_votes_type(fixed_row),
                    party=party,
                    ocd_id=ocd_id,
                    **common_kwargs
                ))

                candidate_index += 1

        return results

    def _fix_row(self, row):
        """
        Fix row that has repeated jurisdiction column
        
        This only occurs in the Polk County Primary results file.
        """
        try:
            if len(row) > 15 and (row[15].strip() in self.POLK_PRECINCTS):
                return row[0:14] + row[16:]
        except AttributeError:
            pass

        return row

    def _is_last_contest_result(self, row, reporting_level):
        cell0 = row[0].strip()
        cell2 = row[2].strip()

        if cell0 == "Total":
            return True

        if reporting_level == 'county' and cell2 == "Total":
            return True

        return False

    def _parse_votes_type(self, row):
        cell0 = row[0].strip()
        cell2 = row[2].strip()

        if cell0 == "Absentee" or cell2 == "Absentee":
            return 'absentee'

        if cell2 == "Election Day":
            return 'election_day'

        return ''


class ExcelPrecinct2014ResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2014 primary precinct-level results

    At the time of authoring, the 2014 general election results were not
    yet available.

    The 2014 primary results files have the following format:

    The first row is a header row.

    All following rows are result rows.

    The columns are as follows:

    * Office
    * Candidate
    * Candidate Party
    * Votes for each precinct with separate columns for polling, absentee and
      total votes.
    * A total column for the county.
    """
    # TODO: See if 2014 general election results follow same format as
    # the primary results

    offices = [
        'U.S. Senator',
        'U.S. Rep.',
        'Governor',
        'Secretary of State',
        'Auditor of State',
        'Treasurer of State',
        'Secretary of Agriculture',
        'Attorney General',
        'State Rep.',
        'State Senator',
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s+Dist\. (?P<district>\d+)){{0,1}}'
        '(?:\s+-\s+(?P<party>Dem|Rep)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE
    )

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        if county == "Van Buren":
            # Van Buren County has an empty initial sheet
            sheet_index = 1
        else:
            sheet_index = 0
        sheet = self._get_sheet(sheet_index=sheet_index)

        for row in self._rows(sheet):
            if row[0].strip() == "RaceTitle":
                jurisdictions = self._parse_jurisdictions(row)
                continue

            results.extend(self._parse_result_row(row, jurisdictions, county,
                county_ocd_id, **base_kwargs))

        return results

    def _parse_jurisdictions(self, row):
        """
        Parse jurisdictions from the initial header row

        Args:
            row (list): List of spreadsheet column values

        Returns:
            List of tuples with the first value representing the name of the
            jurisdiction (precinct or county), the second the reporting level
            of the jurisdiction and the third representing the
            type of vote (absentee, election day, total).

        """
        jurisdictions = []
        for cell in row:
            cell = cell.strip()
            if cell == "":
                continue

            if cell in ("RaceTitle", "CandidateName", "PoliticalPartyName"):
                continue

            jurisdictions.append(self._parse_jurisdiction(cell))

        return jurisdictions

    def _parse_jurisdiction(self, val):
        """
        Parse jurisdiction information from a single column's data

        Args:
            val (str): Spreadsheet cell containing a jurisdiction name.

        Returns:
            A tuple with the first value representing the name of the
            jurisdiction (precinct or county), the second the reporting level
            of the jurisdiction and the third representing the
            type of vote (absentee, election day, total).


        """
        jurisdiction = val
        reporting_level = 'precinct'
        votes_type = 'total'

        if "Absentee" in jurisdiction:
            votes_type = 'absentee'
        elif "Polling" in jurisdiction:
            votes_type = 'election_day'

        if (re.match(r'\w+-Absentee Absentee', jurisdiction) or
                re.match(r'[^-/]+ Total', jurisdiction)):
            reporting_level = 'county'

        return jurisdiction, reporting_level, votes_type

    def _parse_result_row(self, row, jurisdictions, county, county_ocd_id,
            **common_kwargs):
        results = []

        office_cell = row[0].strip()
        m = self._office_re.match(office_cell)
        if m is None:
            logging.info("Skipping office '{}'".format(office_cell))
            return results

        is_primary = (common_kwargs['election_type']  == 'primary')
        party = row[2].strip()
        base_kwargs = {
          'office': m.group('office'),
          'district': m.group('district'),
          'full_name': row[1].strip(),
          'party': party, 
        }
        if is_primary:
            # Not all rows, for instance Write-in pseudo candidates,
            # have a party listed in the 'PoliticalPartyName' column 
            # Use the party listed in the 'RaceTitle' column.
            # Note that these values are Dem, Rep and we don't
            # rewrite, saving that for transforms.
            base_kwargs['primary_party'] = m.group('party')
        base_kwargs.update(common_kwargs)
        votes_start = next(i for i in range(3, len(row)) if row[i] != '')

        for i, votes in enumerate(row[votes_start:]):
            if i >= len(jurisdictions):
                # Ignore trailing empty cells
                break

            jurisdiction, reporting_level, votes_type = jurisdictions[i]
            # To not break backward compatibility, we use an empty string
            # to mean total votes
            if votes_type == 'total':
                votes_type = ''

            results.append(RawResult(
                jurisdiction=jurisdiction,
                reporting_level=reporting_level,
                votes=votes,
                votes_type=votes_type,
                **base_kwargs 
            ))

        return results
