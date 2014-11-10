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
        '20111108__ia__special__general__state_senate__18__precinct.csv',
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
            if mapping['name'] == "Audubon":
                return ExcelPrecinct2010GeneralAudubonResultLoader()
            elif mapping['name'] == "Clinton":
                return ExcelPrecinct2010GeneralClintonResultLoader()
            elif mapping['name'] == "Grundy":
                return ExcelPrecinct2010GeneralGrundyResultLoader()
            elif mapping['name'] == 'Henry':
                return ExcelPrecinct2010GeneralHenryResultLoader()
            elif mapping['name'] == 'Johnson':
                return ExcelPrecinct2010GeneralJohnsonResultLoader()
            elif mapping['name'] == 'Louisa':
                return ExcelPrecinct2010GeneralLouisaResultLoader()
            elif mapping['name'] == 'Poweshiek':
                return ExcelPrecinct2010GeneralPoweshiekResultLoader()
            else:
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
                    county = None
                    if self.mapping['name'] != 'Iowa':
                        county = self.mapping['name']
                    results.append(self._prep_precinct_result(row, county=county))
                elif 'county' in self.source:
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
            return (row['jurisdiction'].strip().upper() in ["TOTALS", "TOTAL"]
                    or row['reporting_level'] == 'state')
        except KeyError:
            # No jurisdiction or reporting_level column means racewide result
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
        jurisdiction = row['jurisdiction']

        # Absentee "precincts" are actually county-level results
        # There are also absentee result counts for specific races, e.g.
        # "ABS & SP BALLOTS HSE 87".  Flag them as absentee votes, but
        # we'll deal with merging them in a transform
        if jurisdiction.lower().startswith("abs"):
            jurisdiction = county
            ocd_id = county_ocd_id
            reporting_level = 'county'
            kwargs['votes_type'] = 'absentee_provisional'
        else:
            reporting_level = 'precinct'
            ocd_id = "{}/precinct:{}".format(county_ocd_id,
                ocd_type_id(jurisdiction))

        kwargs.update({
            'reporting_level': reporting_level,
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

    def _get_sheet_by_name(self, sheet_name, workbook=None):
        if workbook is None:
            workbook = self._get_workbook()

        return workbook.sheet_by_name(sheet_name)

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

        for row in self._rows():
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
                if cell0.lower().endswith('county'):
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

            if cell0 != "" and cell1 != "":
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
        Create result records from a row of results for a precinct

        Args:
            row (list): Data from a row in the spreadsheet.  The first column
                should be the precinct name.  All other columns are vote
                values
            candidates (list): List of candidate full names
            county (string): County name
            county_ocd_id (string): OCD ID for the county

        Returns:
            A list of RawResult models with each dictionary representing the
            votes for a candidate (or pseudo-candidate) for the reporting
            level.

        """
        results = []
        assert len(candidates) == len(row) - 1
        i = 1
        raw_jurisdiction = self._parse_jurisdiction(row[0])
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
                    votes_type=self._votes_type(raw_jurisdiction),
                    jurisdiction=jurisdiction,
                    ocd_id=ocd_id,
                    reporting_level=reporting_level,
                    **common_kwargs
                ))

            i += 1

        return results

    @classmethod
    def _votes_type(cls, jurisdiction):
        if "absentee" in jurisdiction.lower():
            return 'absentee'

        if "provisional" in jurisdiction.lower():
            return 'provisional'

        return None

    @classmethod
    def _parse_jurisdiction(cls, raw_jurisdiction):
        """
        Convert jurisdiction value to a string

        This is needed because some counties just have numbers for precincts
        names. These are interpretted as float values by xlrd.

        Args:
            raw_jurisdiction: Jurisdiction as represented in the spreadsheet
                cell. This should be a string or a float.

        Returns:
            String representing the jurisdiction name

        >>> ExcelPrecinctPre2010ResultLoader._parse_jurisdiction(1.0)
        '1'

        """
        try:
            # Try to convert float to an integer and then a string.
            # E.g. 1.0 -> '1'
            return str(int(raw_jurisdiction))
        except ValueError:
            return raw_jurisdiction


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
                votes_type=self._votes_type(raw_jurisdiction),
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
    def _votes_type(cls, jurisdiction):
        if "absentee" in jurisdiction.lower():
            return 'absentee'

        return None


class ExcelPrecinct2010GeneralResultLoader(ExcelPrecinctResultLoader):
    _county_re = re.compile(r'(?P<county>[A-Za-z ]+) County')
    _office_re = re.compile(r'(?P<office>U\.{0,1}S\.{0,1} Senator|'
        'U\.{0,1}S\.{0,1} Rep(resentative|)|'
        'Governor|Governor/Lt\.{0,1} Governor|'
        'Sec(retary){0,1} of State|Auditor of State|'
        'Treasurer of State|Secretary of Ag(riculture){0,1}|Attorney General|'
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

        vote_breakdowns = self._vote_breakdowns(row, jurisdiction_offset,
            race_offset)

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

        if vote_breakdowns is None:
            # Separate row for each vote breakdown type
            votes = self._get_votes(row, race_offset)
            votes_type = self._votes_type(self._get_votes_type(row, race_offset))
        else:
            # Each vote breakdown type has its own column in the same row
            votes = self._get_total_votes(row, race_offset)
            votes_type = None

        results.append(RawResult(
          votes=votes,
          votes_type=votes_type,
          vote_breakdowns=vote_breakdowns,
          **result_kwargs
        ))

        return results

    def _vote_breakdowns(self, row, jurisdiction_offset, race_offset):
        """
        Get vote breakdowns

        There are two different, but similar layouts for results files.
        One puts total, election day and absentee votes in separate rows.
        Another puts the total, election day and absentee votes
        in separate columns.  An example of this is
        20101102__ia__general__wapello__precinct.xls

        Returns:
            A dictionary of vote breakdowns if there are multiple types of
            results in a single row.  Otherwise None.

        """
        if len(row) <= (5 + jurisdiction_offset):
            return None

        return {
            'election_day': self._get_polling_votes(row, race_offset),
            'absentee': self._get_absentee_votes(row, race_offset),
        }

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
        if val == "Polling":
            return 'election_day'
        elif val == "Absentee":
            return 'absentee'
        else:
            # Otherwise, assume normal vote totals
            return None


class ExcelPrecinct2010GeneralAudubonResultLoader(ExcelPrecinctResultLoader):
    """
    Load precinct-level results from Audubon County in the 2010 general election

    The standardized filename for this results file is
    '20101102__ia__general__audubon__precinct.xls'

    This is needed because the structure of Audubon County's file is
    significantly different from most of the other results files for this
    election.

    """
    offices = [
        "Governor",
        "United States Senator",
        "United States Representative",
        "Secretary of State",
        "Auditor of State",
        "Treasurer of State",
        "Secretary of Agriculture",
        "Attorney General",
        "State Senator",
        "State Representative",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '([-\s]+Dist(rict){{0,1}} (?P<district>\d+)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE
    )

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        # Audubon County's results have a number of sheets in the workbook.
        # We're going to use the one named "Totals -Absentee" because it's the
        # default sheet that opens when I open the workbook in LibreOffice.
        # It also has absentee votes broken down by precinct.
        sheet = self._get_sheet_by_name("Totals -Absentee")
        jurisdictions = []
        office = None
        district = None

        for row in self._rows(sheet):
            cell0 = row[0].strip()
            if cell0 == "Candidates":
                jurisdictions = self._parse_jurisdictions(row)
                continue

            if not len(jurisdictions):
                # Skip header rows
                continue

            if office is None:
                # If we haven't detected an office of interest yet, see if this
                # is a header row for an office of interest
                office, district = self._parse_office(cell0)
                if office is not None:
                    base_kwargs['office'] = office
                    base_kwargs['district'] = district
                # We're done parsing the office and district, move on to the
                # next line
                continue

            results.extend(self._parse_result_row(row, jurisdictions, county,
                county_ocd_id, **base_kwargs))

            if cell0 == "Undervotes":
                # We're done with this office's results. Reset the office
                # and district variables
                office = None
                district = None

        return results

    def _parse_jurisdictions(self, row):
        return [self._clean_jurisdiction_cell(c) for c in row[2:]
                if c.strip() != '']

    def _clean_jurisdiction_cell(self, val):
        # Remove leading and trailing whitespace and compress
        # consecutive whitespace characters down to one
        return re.sub('\s{2,}', ' ', val.strip())

    def _parse_office(self, cell):
        """
        Parse a cell containing office and district information

        Args:
            cell (str): String containing office and district information

        Returns:
            A tuple of strings where the first element is the office name and
            the second element is the district.  If the cell doesn't represent
            an office, or doesn't represent a statewide or federal office, the
            first element will be None.  If the office doesn't have an
            associated district, the second element will be None

        """
        office = None
        district = None

        m = self._office_re.match(cell)
        if m is not None:
            office = m.group('office')
            district = m.group('district')

        return office, district

    def _parse_result_row(self, row, jurisdictions, county, county_ocd_id,
            **common_kwargs):
        results = []
        candidate = row[0].strip()
        party = row[1].strip()
        for jurisdiction, votes in zip(jurisdictions, row[2:]):
            if jurisdiction in ("Total", "Special & Abs"):
                clean_jurisdiction = county
                ocd_id = county_ocd_id
                reporting_level = 'county'
            else:
                ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)
                reporting_level = 'precinct'
                clean_jurisdiction = jurisdiction

            if "Abs" in jurisdiction:
                votes_type = 'absentee'
            elif "Total" in jurisdiction:
                votes_type = None
            else:
                votes_type = 'election_day'

            results.append(RawResult(
                full_name=candidate,
                party=party,
                votes=votes,
                votes_type=votes_type,
                jurisdiction=clean_jurisdiction,
                reporting_level=reporting_level,
                ocd_id=ocd_id,
                **common_kwargs
            ))

        return results


class ExcelPrecinct2010GeneralClintonResultLoader(ExcelPrecinctResultLoader):
    """
    Load precinct-level results from Clinton County for the 2010 general
    election

    The standardized filename for this results file is
    `20101102__ia__general__clinton__precinct.xls`.

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    The file has a single worksheet with rows of the sheet grouped by precinct
    and then by office.

    Precinct groups begin with a heading in the first column of a row with
    a format like "NNNN PRECINCT NAME", for example "0001 BLOOMFIELD DELMAR".

    Office groups follow the format:

    OFFICE NAME
    VOTE FOR NO MORE THAN 1
    Candidate Name (PARTY ABRREV).  .  .  .  .  .  .  .
    WRITE-IN.  .  .  .  .  .  .  .  .  .  .
    Over Votes .  .  .  .  .  .  .  .  .
    Under Votes .  .  .  .  .  .  .  .  .

    Note the period characters following the candidate names.

    Office groups are separated by an empty row.

    Result rows have the following columns (from left to right):

    * Total votes
    * %
    * Absentee
    * Election day

    """
    _precinct_re = re.compile(r'\d{4} .*')

    offices = [
        "US SENATOR",
        "US REP",
        "GOV/LT GOV",
        "SEC OF STATE",
        "AUD OF STATE",
        "TREAS OF STATE",
        "SEC OF AG",
        "ATT GEN",
        "ST SEN",
        "ST REP",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s+DIST (?P<district>\d+)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE)

    _candidate_re = re.compile(r'(?P<candidate>([-\w]+\s*(\w\.\s+){0,1})+)'
        '(\s*\((?P<party>\w+)\)){0,1}')

    votes_types = ['absentee', 'election_day']

    def _results(self, mapping):
        results = []
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        # Unlike other results files for Iowa, all rows represent
        # a precinct result
        base_kwargs['reporting_level'] = 'precinct'
        office = None
        district = None

        for row in self._rows():
            try:
                cell0 = row[0].strip()
            except AttributeError:
                # Some values in the first cell are floats, so they don't
                # have strip, but we should skip these anyway
                continue

            if cell0 == '' or "vote for no" in cell0.lower():
                # Ignore empty rows or the instructional rows within
                # an office group
                continue

            m = self._precinct_re.match(cell0)
            if m is not None:
                base_kwargs['jurisdiction'] = cell0
                base_kwargs['ocd_id'] = '{}/precinct:{}'.format(county_ocd_id,
                    ocd_type_id(base_kwargs['jurisdiction']))
                continue

            if office is None:
                # If we aren't in an office block, see if the row
                # matches an office name heading.
                office, district = self._parse_office(row)

                # This guard also helps us skip header lines like
                # "PREC REPORT-GROUP DETAIL"
                # and "RUN DATE:12/16/10 03:16 PM"

                if office:
                    # An office was detected
                    base_kwargs['office'] = office
                    base_kwargs['district'] = district

                # Skip to the next row.
                continue

            # If we've gotten this far without short-circuiting, our row must
            # be a results row.
            results.extend(self._parse_result_row(row, **base_kwargs))

            if cell0.startswith("Under Votes"):
                # The under votes row is the last result row in an office group.
                # Reset the office.
                office = None

        return results

    def _parse_office(self, row):
        office = None
        district = None
        cell0 = row[0].strip()
        m = self._office_re.match(cell0)
        if m:
            office = m.group('office')
            district = m.group('district')
        return office, district

    def _parse_result_row(self, row, **base_kwargs):
        results = []
        candidate, party = self._parse_candidate(row[0].strip())
        results.append(RawResult(
          full_name=candidate,
          party=party,
          votes=row[1],
          vote_breakdowns=self._vote_breakdowns(row),
          **base_kwargs
        ))

        return results

    def _vote_breakdowns(self, row):
        votes = row[3:]
        return {vt: v for vt, v in zip(self.votes_types, votes)}

    def _parse_candidate(self, cell):
        candidate = None
        party = None
        m = self._candidate_re.match(cell)
        if m:
            # Need to call strip because the re might greedily
            # grab trailing whitespace between the candidate
            # name and the party
            candidate = m.group('candidate').strip()
            party = m.group('party')
        return candidate, party


class ExcelPrecinct2010GeneralGrundyResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2010 general election precinct-level results for Grundy County

    This file has the standardized filename
    20101102__ia__general__grundy__precinct.xls

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    """
    offices = [
        'US SENATOR',
        'US REPRESENTATIVE',
        'GOVERNOR/LIEUTENANT GOVERNOR',
        'SECRETARY OF STATE',
        'AUDITOR OF STATE',
        'TREASURER OF STATE',
        'SECRETARY OF AGRICULTURE',
        'ATTORNEY GENERAL',
        'STATE REPRESENTATIVE',
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s*-\s+DIST\.\s+(?P<district>\w+)){{0,1}}'.format(offices='|'.join(offices)))

    _candidate_re = re.compile(r'(?P<candidate>[^\(]+)(\s*\((?P<write_in>WRITE-IN)\)){0,1}')

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        office = None
        district = None

        for row in self._rows():
            cell0 = row[0].strip()

            if cell0 == '':
                # Skip lines with nothing in the leading cell
                continue

            if cell0 == "CANDIDATES":
                jurisdictions = self._parse_jurisdictions(row)
                continue

            if office is None:
                office, district = self._parse_office(row)
                if office:
                    base_kwargs['office'] = office
                    base_kwargs['district'] = district
                continue

            results.extend(self._parse_result_row(row, jurisdictions,
                county, county_ocd_id, **base_kwargs))

            if cell0 == 'TOTAL':
                office = None
                district = None

        return results

    def _parse_jurisdictions(self, row):
        return [c.strip() for c in row[1:] if c.strip() != '']

    def _parse_office(self, row):
        office = None
        district = None
        m = self._office_re.match(row[0].strip())
        if m:
            office = m.group('office')
            district = m.group('district')
        return office, district

    def _parse_result_row(self, row, jurisdictions, county, county_ocd_id,
            **base_kwargs):
        results = []
        candidate, write_in = self._parse_candidate(row[0].strip())
        for i, jurisdiction in enumerate(jurisdictions):
            votes = row[i+1]
            if not votes:
                votes = 0

            if "ABS" in jurisdiction:
                votes_type = 'absentee'
            else:
                votes_type = None

            if jurisdiction in ("ABS", "TOTAL"):
                reporting_level = 'county'
                jurisdiction = county
                ocd_id = county_ocd_id
            else:
                reporting_level = 'precinct'
                ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)

            results.append(RawResult(
                jurisdiction=jurisdiction,
                ocd_id=ocd_id,
                reporting_level=reporting_level,
                full_name=candidate,
                write_in=write_in,
                votes=votes,
                votes_type=votes_type,
                **base_kwargs
            ))

        return results

    def _parse_candidate(self, cell):
        m = self._candidate_re.match(cell)
        candidate = m.group('candidate').strip()
        write_in = m.group('write_in')
        return candidate, write_in


class ExcelPrecinct2010GeneralHenryResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2010 generl election precinct-level results for Henry County

    This file has the standardized filename
    '20101102__ia__general__henry__precinct.xls',

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    """
    offices = [
        "United States Senator",
        "United States Representative",
        "Governor",
        "Secretary of State",
        "Auditor of State",
        "Treasurer of State",
        "Secretary of Agriculture",
        "Attorney General",
        "State Representative",
    ]

    _district_re = re.compile(r'District\s+(?P<district>\d+)')

    _candidate_re = re.compile(r'(?P<candidate>[^\(]+)(\s*\((?P<party>\w+)\)){0,1}')

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        office = None
        district = None
        jurisdictions = None

        for row in self._rows():
            cell0 = row[0].strip()

            if cell0 == '' and jurisdictions is None:
                if row[1].strip() == 'Absentee':
                    jurisdictions = self._parse_jurisdictions(row)

                continue

            if cell0 in self.offices:
                office = cell0
                base_kwargs['office'] = office
                district = None
                base_kwargs['district'] = None
                continue

            if office is not None and district is None:
                m = self._district_re.match(cell0)
                if m is not None:
                    district = m.group('district')
                    base_kwargs['district'] = district
                    continue

            if "Vote for" in cell0:
                continue

            if cell0 in ("", "Grand Total Votes Cast", "(Vote for One)"):
                continue

            if office is None:
                continue

            results.extend(self._parse_result_row(row, jurisdictions, county,
                county_ocd_id, **base_kwargs))

            if cell0 == "Under Votes":
                office = None
                district = None

        return results

    def _parse_jurisdictions(self, row):
        return [c.strip() for c in row if c.strip() != '']

    def _parse_result_row(self, row, jurisdictions, county, county_ocd_id,
            **base_kwargs):
        results = []
        candidate, party = self._parse_candidate(row[0].strip())

        for i, jurisdiction in enumerate(jurisdictions):
            if jurisdiction == "Absentee":
                votes_type = 'absentee'
            else:
                votes_type = None

            if jurisdiction in ("Absentee", "TOTAL"):
                jurisdiction = county
                reporting_level = 'county'
                ocd_id = county_ocd_id
            else:
                reporting_level = 'precinct'
                ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)

            if candidate == 'Write-In':
                write_in = 'Write-In'
            else:
                write_in = None

            votes = row[i+1]
            if not votes:
                votes = 0

            results.append(RawResult(
                jurisdiction=jurisdiction,
                ocd_id=ocd_id,
                reporting_level=reporting_level,
                full_name=candidate,
                party=party,
                votes=votes,
                votes_type=votes_type,
                write_in=write_in,
                **base_kwargs
            ))

        return results

    def _parse_candidate(self, cell):
        m = self._candidate_re.match(cell)
        candidate = m.group('candidate').strip()
        party = m.group('party')
        return candidate, party


class ExcelPrecinct2010GeneralJohnsonResultLoader(ExcelPrecinct2010GeneralClintonResultLoader):
    """
    Load precinct-level results from Johnson County for the 2010 general
    election

    The standardized filename for these results is
    '20101102__ia__general__johnson__precinct.xls'

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    The results file has a very similar structure to that of Clinton county.

    The main differences are:

    * Different case and abbrevations in office name
    * Different case, format in "Vote for no more than" text

    """
    offices = [
        "UNITED STATES SENATOR",
        "UNITED STATES REPRESENTATIVE",
        "GOVERNOR AND LIEUTENANT GOVERNOR",
        "SECRETARY OF STATE",
        "AUDITOR OF STATE",
        "TREASURER OF STATE",
        "SECRETARY OF AGRICULTURE",
        "ATTORNEY GENERAL",
        "STATE SENATOR SENATE",
        "STATE REPRESENTATIVE HOUSE",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s+DISTRICT (?P<district>\d+)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE)

    votes_types = ['election_day', 'absentee']


class ExcelPrecinct2010GeneralLouisaResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2010 generl election precinct-level results for Louisa County

    This file has the standardized filename
    '20101102__ia__general__louisa__precinct.xls',

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    """
    offices = [
        "US Senator",
        "US Representative",
        "Governor/ Lieutenant Governor",
        "Secretary of State",
        "Auditor of State",
        "Treasurer of State",
        "Secretary of Agriculture",
        "Attorney General",
        "State Rep",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '(\s+Dist(rict){{0,1}}\s+(?P<district>\d+)){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE
    )

    parties = [
        "REP",
        "DEM",
        "LIB",
        "CONS",
        "IOWA",
        "SOC",
        "NBP",
    ]
    _party_re = re.compile('\s+(?P<party>{parties})$'.format(parties='|'.join(parties)))

    _votes_start = 5
    """Index where the votes start"""

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        base_jurisdiction = None

        for i, row in enumerate(self._rows()):
            if i == 0:
                offices = self._parse_office_row(row)
                continue

            if i == 1:
                candidates = self._parse_candidates_row(row, offices)
                continue

            if row[2] == '':
                # Skip empty rows.  We know a row is empty if it doesn't
                # have votes in the third column
                base_jurisdiction = None
                continue

            if row[1] != '' and 'Abs' not in row[1]:
                base_jurisdiction = row[1]

            results.extend(self._parse_result_row(row, offices, candidates,
                base_jurisdiction, county, county_ocd_id, **base_kwargs))

        return results

    def _parse_office_row(self, row):
        offices = []
        office = None
        district = None

        for cell in row[5:]:
            if office and cell == '':
                offices.append((office, district))
            else:
                m = self._office_re.match(cell)
                if m is not None:
                    office = m.group('office')
                    district = m.group('district')
                    offices.append((office, district))
                else:
                    office = None
                    district = None

        return offices

    def _parse_candidates_row(self, row, offices):
        candidates = []
        for i in range(len(offices)):
            candidates.append(self._parse_candidate(row[i+self._votes_start]))

        return candidates

    def _parse_candidate(self, cell):
        candidate = None
        party = None

        m = self._party_re.search(cell)
        if m is not None:
            party = m.group('party')
            candidate = self._party_re.sub('', cell)
        else:
            candidate = cell

        return candidate, party

    def _parse_result_row(self, row, offices, candidates, base_jurisdiction,
            county, county_ocd_id, **base_kwargs):
        """
        Create result records from a row of results for a precinct

        Args:
            row (list): Data from a row in the spreadsheet.  The first column
                should be the precinct name.  All other columns are vote
                values
            offices (list): List of tuples where the first item is the office
                and the second is the district
            candidates (list): List of tuples where the first item is the
                candidate name, the second the candidate party
            base_jurisdiction (string): Base jurisdiction since the total line
                is empty
            county (string): County name
            county_ocd_id (string): OCD ID for the county

        Returns:
            A list of RawResult models with each dictionary representing the
            votes for a candidate (or pseudo-candidate) for the reporting
            level.

        """
        results = []
        jurisdiction = row[1].strip()
        if 'Abs' in jurisdiction:
            votes_type = 'absentee'
        elif jurisdiction == '':
            votes_type = None
        else:
            votes_type = 'election_day'

        if jurisdiction == '':
            jurisdiction = base_jurisdiction

        if jurisdiction == 'TOTAL':
            jurisdiction = county
            ocd_id = county_ocd_id
            reporting_level = 'county'
        else:
            ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)
            reporting_level = 'precinct'

        for i, office_district in enumerate(offices):
            office, district = office_district
            candidate, party = candidates[i]
            votes = row[i+self._votes_start]
            results.append(RawResult(
                jurisdiction=jurisdiction,
                ocd_id=ocd_id,
                reporting_level=reporting_level,
                full_name=candidate,
                party=party,
                office=office,
                district=district,
                votes=votes,
                votes_type=votes_type,
                **base_kwargs
            ))

        return results


class ExcelPrecinct2010GeneralPoweshiekResultLoader(ExcelPrecinctResultLoader):
    """
    Parse 2010 generl election precinct-level results for Poweshiek County

    This file has the standardized filename
    '20101102__ia__general__poweshiek__precinct.xls',

    A separate class is needed because the structure of this county's file is
    significantly different from most of the other results files for this
    election.

    """
    offices = [
      "United States Senator",
      "U.S. Rep",
      "Governor Lt. Governor",
      "Secretary of State",
      "Auditor of State",
      "Treasurer of State",
      "Secretary of Agriculture",
      "Attorney General",
      "State Rep",
    ]
    _office_re = re.compile('(?P<office>{offices})'
        '((\s+District){{0,1}}\s+(?P<district>\d+)(th){{0,1}}(\s+Dist){{0,1}}){{0,1}}'.format(offices='|'.join(offices)),
        re.IGNORECASE
    )
    parties = [
        "REP",
        "DEM",
        "LIB",
        "SWP",
        "IAP",
        "NBP",
    ]
    _party_re = re.compile('\s+-\s+(?P<party>{parties})$'.format(parties='|'.join(parties)))

    def _results(self, mapping):
        results = []
        county = mapping['name']
        county_ocd_id = mapping['ocd_id']
        base_kwargs = self._build_common_election_kwargs()
        base_kwargs['votes_type'] = None

        for i, row in enumerate(self._rows()):
            if i == 0:
                row0 = row
                continue

            if i == 1:
                offices = self._parse_office_row(row0, row)
                continue

            if i == 2:
                candidates = self._parse_candidates_row(row)

            cell0 = row[0].strip()

            if cell0 == "Absentee:":
                base_kwargs['votes_type'] = 'absentee'
                continue

            if cell0 == "Totals":
                base_kwargs['votes_type'] = None

            results.extend(self._parse_result_row(row, offices, candidates,
                county, county_ocd_id, **base_kwargs))

        return results

    def _parse_office_row(self, row0, row1):
        offices = []
        office = None
        district = None
        # The office names are spread across two rows.  Merge them.
        row = [(str(c0).strip() + ' ' + str(c1)).strip() for c0, c1 in zip(row0[1:], row1[1:])]

        for cell in row:
            if office and cell == '':
                offices.append((office, district))
            else:
                m = self._office_re.match(cell)
                if m is not None:
                    office = m.group('office')
                    district = m.group('district')
                    offices.append((office, district))
                else:
                    office = None
                    district = None

        return offices

    def _parse_candidates_row(self, row):
        candidates = []
        for cell in row[1:]:
            candidates.append(self._parse_candidate(cell))

        return candidates

    def _parse_candidate(self, cell):
        candidate = None
        party = None

        m = self._party_re.search(cell)
        if m is not None:
            party = m.group('party')
            candidate = self._party_re.sub('', cell)
        else:
            candidate = cell

        return candidate, party

    def _parse_result_row(self, row, offices, candidates, county, county_ocd_id,
            **base_kwargs):
        results = []
        jurisdiction = row[0].strip()

        # Note: setting the votes_type kwarg is handled outside of this method

        if "Total" in jurisdiction:
            jurisdiction = county
            ocd_id = county_ocd_id
            reporting_level = 'county'
        else:
            ocd_id = county_ocd_id + '/' + ocd_type_id(jurisdiction)
            reporting_level = 'precinct'

        for office_dist, candidate_party, votes in zip(offices, candidates,
                row[1:]):
            office, district = office_dist
            candidate, party = candidate_party
            if candidate == "2010 General Election":
                # Skip "spacer" columns
                continue

            if votes == '':
                # Don't create results when they're not available for a
                # particular race.
                continue

            if candidate == "Write-In":
                write_in = "Write-In"
            else:
                write_in = None

            results.append(RawResult(
                jurisdiction=jurisdiction,
                ocd_id=ocd_id,
                reporting_level=reporting_level,
                office=office,
                district=district,
                full_name=candidate,
                party=party,
                write_in=write_in,
                votes=votes,
                **base_kwargs
            ))

        return results


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

        return None


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
        votes_type = None

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

            results.append(RawResult(
                jurisdiction=jurisdiction,
                reporting_level=reporting_level,
                votes=votes,
                votes_type=votes_type,
                **base_kwargs
            ))

        return results
