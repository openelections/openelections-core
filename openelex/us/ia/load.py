import re

import unicodecsv
import xlrd

from openelex.base.load import BaseLoader
from openelex.lib.text import ocd_type_id
from openelex.models import RawResult
from openelex.us.ia.datasource import Datasource

import logging

class LoadResults(object):
    """
    Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    # TODO: Don't skip these.  They're being skipped because the PDF results
    # haven't been converted yet in favor of higher-value files
    SKIP_FILES = [
        '20010612__ia__special__general__state_house__85__county.csv',
        '20010612__ia__special__general__state_senate__43__county.csv',
        '20011106__ia__special__general__state_house__82__county.csv',
        '20020122__ia__special__general__state_house__28__county.csv',
        '20020219__ia__special__general__state_senate__39__county.csv',
        '20020312__ia__special__general__state_senate__10__state.csv',
        '20021105__ia__general__precinct.csv', 
        '20030114__ia__special__general__state_senate__26__county.csv',
        '20030211__ia__special__general__state_house__62__county.csv',
        '20030805__ia__special__general__state_house__100__county.csv',
        '20030826__ia__special__general__state_house__30__county.csv',
        '20040203__ia__special__general__state_senate__30__county.csv',
        '20091124__ia__special__general__state_house__33__precinct.csv',
    ]

    def run(self, mapping):
        generated_filename = mapping['generated_filename']

        print(generated_filename)
        if generated_filename in self.SKIP_FILES:
            loader = SkipLoader()
        elif ('precinct' in generated_filename and
                generated_filename.endswith('xls')):
            loader = ExcelPrecinctResultLoader()
        elif 'pre_processed_url' in mapping:
            loader = PreprocessedResultsLoader()

        loader.run(mapping)

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
                    results.append(self._prep_precinct_result(row))
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
            kwargs['jurisdiction'] = self._clean_jurisdiction(row['jurisdiction'].strip())
            
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

    def _clean_jurisdiction(self, jurisdiction):
        """Clean the jurisdiction field a bit"""
        # TODO: Do this in a Raw Transform
        # See https://github.com/openelections/core/issues/206
        if jurisdiction == "VanBuren":
            return "Van Buren"
        else:
            return jurisdiction

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        kwargs['reporting_level'] = 'county'
        # Use kwargs['jurisdiction'] instead of row['jurisdiction'] because
        # the value in the kwargs dict will already have been cleaned to
        # enable proper lookup.
        kwargs['ocd_id'] = self._lookup_county_ocd_id(kwargs['jurisdiction'])

        return RawResult(**kwargs)

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        county_ocd_id = self._lookup_county_ocd_id(row['county'])
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

    def _get_sheet(self, workbook=None):
        """Return the sheet of interest from the workbook"""
        if workbook is None:
            workbook = self._get_workbook()

        return workbook.sheets()[0]

    def _get_workbook(self):
        return xlrd.open_workbook(self._xls_file_handle())


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
                absentee = None
                provisional = None
                common_kwargs = {
                  'office': office,
                  'district': district,
                }
                common_kwargs.update(base_kwargs)
                continue

            if cell0 != " "and cell1 != "":
                # Result row

                if cell0 == 'ABSENTEE PRECINCT':
                    absentee = [row[i] for i in range(1, len(row))
                                if candidates[i-1] != ""]
                elif cell0 == 'PROVISIONAL PRECINCT':
                    provisional = [row[i] for i in range(1, len(row))
                                if candidates[i-1] != ""]
                else:
                    row_results = self._parse_result_row(row, candidates,
                        county, county_ocd_id, **common_kwargs)

                    if cell0 == "Totals":
                        # Add absentee, provisional votes to vote breakdowns
                        # for county totals
                        row_results = self._add_vote_breakdowns(row_results,
                            absentee, provisional)

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
        jurisdiction = row[0]
        if jurisdiction == "Totals":
            # Total of all precincts is a county-level result 
            jurisdiction = county
            ocd_id = county_ocd_id
            reporting_level = 'county'
        else:
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
                    jurisdiction=jurisdiction,
                    ocd_id=ocd_id, 
                    reporting_level=reporting_level,
                    **common_kwargs
                ))

            i += 1

        return results

    def _add_vote_breakdowns(self, results, absentee, provisional):
        i = 0
        for result in results:
            result.vote_breakdowns = {}
            result.vote_breakdowns['absentee'] = absentee[i]

            if provisional:
                result.vote_breakdowns['provisional'] =  provisional[i]
            i += 1

        return results


class ExcelPrecinctPost2010ResultLoader(ExcelPrecinctResultLoader):
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
            cell2 = row[2].strip()

            if cell0 == "Race":
                # Skip the first 3 columns, which are column headings,
                # "Race", "County", "Precinct"
                candidates = row[3:]
                office = None
                district = None
                party = None
                continue
            elif cell3 == "ABSENTEE":
                absentee = row[3:]
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
                common_kwargs.update(base_kwargs)


            row_results = self._parse_results_row(row, candidates, county,
                county_ocd_id, **common_kwargs)

            if cell0 == "Grand Totals":
                row_results = self._add_vote_breakdowns(row_results,
                    absentee)

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

    def _parse_results_row(self, row, candidates, county, county_ocd_id,
        **common_kwargs):
        pass
