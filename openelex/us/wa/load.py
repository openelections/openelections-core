import logging
import os
import re
import unicodecsv
import xlrd

from pymongo import errors

"""
Importing errors from pymongo allows us to except the specific pymongo
error which is raised when we try to perform an empty bulk insert.
We except the error because we provide our own, slightly more in-depth
error message instead.

"""

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import slugify
from .datasource import Datasource

"""
Washington state elections have CSV and XLSX result files.
Results from < 2007 have a different format than those <= 2008.
Actually, most every file has a different format.

TO DO:

1.) Add in .xls(x) support (Aug 12, 2014)
2.) Fix memory issues [class WALoaderPrecincts, LINE# 230] (Aug 14, 2014)
3.) Takes forever on large files (Aug 15, 2014)

NOTES:

1.) Loader uses two normalizing classes that normalize parts of the data.
    In particular, we use ColumnMatch to normalize the headers of different
    files whose headers are generally the same, but differ in the wording.

    For example, some files will have all the same fields, but name them
    slightly differently. In one file, the column that holds the candidate's
    name might be "CANDIDATE_FULL_NAME", while another might be
    "candidate name". Because of this, we use regex to test the header row
    to find the correct field.

    NormalizeRaces takes the race data and then matches it against
    `target_offices`, found in the WABaseLoader class. We do this because
    Washington will preface some of the positions (e.g. Governor) with
    "Washington State", and some files call the lower chamber "Representative"
    or "Legislator" or use the word "House" while referring to the same
    position.

"""

# Instantiate logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class LoadResults(object):

    """
    Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        """
        generated_filename will return a filename similar to this:
            `20101107__wa__general__precinct.csv`

        election will return a filename similar to this:
            `20101102__wa__general__precinct`

        """

        generated_filename = mapping['generated_filename']
        election = mapping['election']

        """
        bad_filenames[] holds the list of files who have content that's not
        actual information (e.g. a mess of HTML from a file that moved on the
        remote server).

        """

        bad_filenames = [

            # The below are .csv

            '20090818__wa__primary__pierce__county.csv',
            '20090818__wa__primary__ferry__county.csv',
            '20090818__wa__primary__wahkiakum__county.csv',
            '20090818__wa__primary__whatcom__county.csv',
            '20090818__wa__primary__pend_oreille__county.csv',
            '20090818__wa__primary__kitsap__county.csv',
            '20090818__wa__primary__kittitas__county.csv',

            # The the below are .xls

            '20080219__wa__primary__benton__precinct.xls',
            '20081104__wa__general__kittitas__precinct.xls',
            '20081104__wa__general__precinct.xls',
            '20091103__wa__general__clark__precinct.xls',
            '20100817__wa__primary__state_legislative.xls'
            '20120807__wa__primary__congressional_district.xls',
            '20120807__wa__primary__state_legislative.xls',
            '20121106__wa__general__congressional_district.xls',
            '20121106__wa__general__state_legislative.xls',
            '20080219__wa__primary__adams__precinct.xls',
            '20101102__wa__general__kittitas___precinct.xls'
        ]

        """
        Could try using `generated_filename.split(.)[-1]` instead of
        os.path.splitext(election)[-1], since all filenames are
        standardized. This would, of course, break if the file path includes
        a full stop.

        """

        # If files are 'bad', skip them
        if any(x in generated_filename for x in bad_filenames):
            print('File {0} does not contain .csv data'
                  .format(generated_filename))
            loader = SkipLoader()

        # If files are .xls(x), skip them
        elif os.path.splitext(
                generated_filename)[-1].lower() == '.xls' or os.path.splitext(
                generated_filename)[-1].lower() == '.xlsx':
            loader = WALoadExcel()

        elif os.path.splitext(generated_filename)[-1].lower() == '.txt':

            """
            We run into issues where King County provides > 1 million line
            .txt files that break my machine's memory. We definitely need to
            refactor, but for the moment we'll pass over said files.

            """

            print 'Cannot do anything with {0}'.format(generated_filename)
            loader = SkipLoader()
        elif re.search('precinct', generated_filename):
            loader = WALoaderPrecincts()
        elif any(s in election for s in [
                '2000',
                '2001',
                '2002',
                '2003',
                '2004',
                '2005',
                '2006']):
            loader = WALoaderPre2007()
        elif os.path.splitext(generated_filename)[-1].lower() != '.xls':
            loader = WALoaderPost2007()
        else:
            loader = SkipLoader()

        """
        * UnboundLocalError: File passes through the elif statements, but is
          not a file we have a loader class set up to handle at this point, so
          loader.run(mapping) is called before it's mentioned

        * IOError: File in quesiton does not exist. Seen when the mapping
          a file path that recieved a 404 error

        * unicodecsv.Error: Similar to UnboundLocalError, this error means
          that the loader tried running but the csv parser could not parse
          the file because of a null byte. See:
          https://github.com/jdunck/python-unicodecsv/blob/master/unicodecsv/test.py#L222

        * errors.InvalidOperation: When a file has no useful data, RawResult
          is empty and mongodb refuses to load it.

        Because of the if/else flow, sometimes we'll end up with multiple
        UnboundLocalErrors. This should be changed so we only get the error
        once.
        
        """

        try:
            loader.run(mapping)
        except UnboundLocalError:
            logger.error(
                '\tERROR: Unsupported file type ({0})'
                .format('UnboundLocalError'))
        except IOError:
            logger.error(
                '\tERROR: File "{0}" does not exist'
                .format(generated_filename))
        except unicodecsv.Error:
            logger.error(
                '\tERROR: Unsupported file type ({0})'
                .format('unicodecsv.Error'))
        except errors.InvalidOperation:
            logger.error('\tNo raw results loaded')


class WABaseLoader(BaseLoader):
    datasource = Datasource()

    """
    target_offices are the offices that openeelections is looking for.
    This set() is a master list that all of the rows in the .csv and .xls(x)
    files are matched against (after being normalized).

    """

    target_offices = set([
        'President',
        'U.S. Senator',
        'U.S. Representative',
        'Governor',
        'Secretary of State',
        'Superintendent of Public Instruction',
        'State Senator',
        'State Representative',
        'Lt. Governor',
        'Governor',
        'Treasurer',
        'Auditor',
        'State Superintendent of Public Instruction',
        'Attorney General',
        'Commissioner of Public Lands'
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?
        This should be implemented in subclasses.

        """
        return False


class ColumnMatch:

    """
    New methods to normalize headers should follow this structure:

        @classmethod
        def _normalize_*(cls, header):


            # Some sort of examples of what words the regex tests for
            # Example 1
            # Example 2
            # etc

            regex = re.compile(regex, flags)

            return [
                m.group(0) for l in header for m in [
                    regex.search(l)] if m][0]

            # OR

            return filter(lambda x: regex.search(x), header)[0]

    The filter(lambda...) is equivalent to the list comprehension, but is
    more readable. I've chosen to go with the lambda function because of
    readability, even though the list comprehension is much quicker.

    See: https://gist.github.com/EricLagerg/152e402e45088266e189

        * For 1 iteration:

          lambda: 0.000517129898071
          list  : 0.000169992446899 <

        * For 10:

          lambda: 0.00197100639343
          list  : 0.00181102752686 <

        * For 100:

          lambda: 0.0169317722321
          list  : 0.0162620544434 <

        * For 1,000:

          lambda: 0.15958404541 <
          list  : 0.161957025528

    The `header` arg will be (or at least currently is) a list of all the
    matches from testing the .csv file's header field.

    """

    @classmethod
    def _normalize_party(cls, header):
        """
        Regex examples:

        party = true
        party_code = true
        party code = true

        """

        regex = re.compile(
            r'.*(^party$|party.*code|candidate(_|\s+)party(_|\s)id).*',
            re.IGNORECASE)

        """
        `return filter(lambda x: regex.search(x), header)[0]`
        does the same as the below list comprehension

        """

        return filter(lambda x: regex.search(x), header)[0]

    @classmethod
    def _normalize_candidate(cls, header):
        """
        Regex examples:

        candidate = true
        candidate_name = true
        candidate_id = false
        candidate_full_name = true

        """

        regex = re.compile('.*(candidate.*name|candidate).*', re.IGNORECASE)

        return filter(lambda x: regex.search(x), header)[0]

    @classmethod
    def _normalize_contest(cls, header):
        """
        Regex examples:

        contest = true
        race = true
        contest_name = true
        contest_id = false

        """

        regex = re.compile(
            '.*(^contest$|race|(contest.*(title|name))).*',
            re.IGNORECASE)

        return filter(lambda x: regex.search(x), header)[0]

    @classmethod
    def _normalize_precinct(cls, header):
        """
        Regex examples:

        precinct = true
        precinct_name = true
        precinct name = true

        """

        regex = re.compile('.*(precinct|precinct.*name).*', re.IGNORECASE)

        return filter(lambda x: regex.search(x), header)[0]

    @classmethod
    def _normalize_votes(cls, header):
        """
        Regex examples:

        number of votes for = true
        votes = true
        count = true
        total number of votes = false

        """

        regex = re.compile(
            '.*(.*vote.*for|^vote|^count$|total_votes|total.*votes).*',
            re.IGNORECASE)

        return filter(lambda x: regex.search(x), header)[0]


cm = ColumnMatch()


class NormalizeRaces:

    @classmethod
    def _is_match(cls, string):
        """
        Would appear to return a boolean, but returns the normalized
        race result as per `target_offices`.

        Although we should not provide 'N/A' in places where we don't have
        valid data (e.g. if no party is stated, we simply don't provide the
        party value instead of providing 'N/A' or a blank value), returning
        'N/A' here will result in us skipping the row, since this class is
        and only should be used *only* in the `self._skip_row` methods.

        Returning anything other than one of the values in `target_offices`
        will result in the row being skipped. Since 'N/A' isn't in
        `target_offices`, we're fine.

        """

        presidential_regex = re.compile('president', re.IGNORECASE)
        senate_regex = re.compile('(senate|senator)', re.IGNORECASE)
        house_regex = re.compile('(house|representative)', re.IGNORECASE)
        governor_regex = re.compile('governor', re.IGNORECASE)
        treasurer_regex = re.compile('treasurer', re.IGNORECASE)
        auditor_regex = re.compile('auditor', re.IGNORECASE)
        sos_regex = re.compile('secretary', re.IGNORECASE)
        lt_gov_regex = re.compile(r'(lt|Lt|Lieutenant)', re.IGNORECASE)
        ospi_regex = re.compile(
            'superintendent of public instruction',
            re.IGNORECASE)
        ag_regex = re.compile('attorney general', re.IGNORECASE)
        wcpl_regex = re.compile('commissioner of public lands', re.IGNORECASE)
        local_regex = re.compile(
            r'(\bState\b|Washington|Washington State|Local|'
            'Legislative District)',
            re.IGNORECASE)
        national_regex = re.compile(
            r'(U.S.|US|Congressional|National|United States|U. S.)',
            re.IGNORECASE)

        if re.search(presidential_regex, string):
            return 'President'
        elif re.search(senate_regex, string):
            if re.search(local_regex, string):
                return 'State Senate'
            elif re.search(national_regex, string):
                return 'U.S. Senate'
            else:
                return 'N/A'
        elif re.search(house_regex, string):
            if re.search(local_regex, string):
                return 'State Representative'
            elif re.search(national_regex, string):
                return 'U.S. Representative'
            else:
                return 'N/A'
        elif re.search(lt_gov_regex, string):
            return 'Lt. Governor'
        elif re.search(sos_regex, string):
            return 'Secretary of State'
        elif re.search(governor_regex, string):
            return 'Governor'
        elif re.search(treasurer_regex, string):
            return 'Treasurer'
        elif re.search(auditor_regex, string):
            return 'Auditor'
        elif re.search(ospi_regex, string):
            return 'Superintendent of Public Instruction'
        elif re.search(ag_regex, string):
            return 'Attorney General'
        elif re.search(wcpl_regex, string):
            return 'Commissioner of Public Lands'
        else:
            return 'N/A'

nr = NormalizeRaces()


class SkipLoader(WABaseLoader):

    """
    A hacky workaround for all those pesky files that we can't do anything
    with right now.

    If we don't implement a loader class that skips over a file, then we end
    up with a long chain of UnboundLocalErrors because loader is being called
    before it's actually being defined.

    Because the base/loader.py file requires us to have a load method (See:
    https://github.com/openelections/core/blob/dev/openelex/base/load.py#L83)
    we create a load method that prints an error message and then passes, thus
    skipping the problem file.

    """

    def load(self):
        logger.error('\tNothing we can do with this file')
        pass


class WALoaderPrecincts(WABaseLoader):

    """
    Parse Washington election results for all precinct files.
    This class uses the ColumnMatch class to normalize the column
    headers.

    """

    def load(self):

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        results = []

        with self._file_handle as csvfile:
            no_district = 0
            no_party = 0
            reader = unicodecsv.DictReader(
                csvfile, encoding='latin-1', delimiter=',')
            self.header = [x.replace('"', '') for x in reader.fieldnames]
            for row in reader:
                if self._skip_row(row):
                    continue
                else:
                    votes = int(row[cm._normalize_votes(self.header)].strip())
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    rr_kwargs.update({
                        'reporting_level': 'precinct',
                        'votes': votes,
                        'county_ocd_id': self.mapping['ocd_id']
                    })
                    try:
                        rr_kwargs.update({
                            'party': row[cm._normalize_party(
                                self.header)].strip()
                        })
                    except IndexError:
                        no_party += 1
                    try:
                        rr_kwargs.update({
                            'district': '{0} {1}'.format(
                                nr._is_match(
                                        row[cm._normalize_contest(
                                            self.header)]), [
                                        int(s) for s in row[
                                            cm._normalize_contest(
                                                self.header)].strip()
                                        if s.isdigit()][0])})
                    except IndexError:
                        no_district += 1
                    results.append(RawResult(**rr_kwargs))
            if no_district != 0:
                logger.error(
                    'Index Error in WALoaderPost2007 while updating'
                    ' district: {0} lines had no district'
                    ' number').format(no_district)
            if no_party != 0:
                logger.error('No party preference listed for any candidate')

        """
        Many county files *only* have local races, such as schoolboard or
        fire chief races. Since openstates does not want these results,
        the entire files end up being skipped. To clarify the error message,
        we print our own if RawResult tries to insert nothing into mongodb

        """

        try:
            RawResult.objects.insert(results)
        except errors.InvalidOperation:
            logger.error('\tNo raw results loaded')

    def _skip_row(self, row):
        return nr._is_match(
            row[cm._normalize_contest(self.header)]) not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'office': row[cm._normalize_contest(self.header)].strip(),
            'jurisdiction': row[cm._normalize_precinct(self.header)].strip(),
        }

    def _build_candidate_kwargs(self, row):
        full_name = row[cm._normalize_candidate(self.header)].strip()

        return {
            'full_name': full_name,
            'name_slug': slugify(full_name, substitute='-')
        }

"""
I don't know why this class is set up different from the rest.
I should fix this.

"""


class WALoaderPre2007(WABaseLoader):

    """
    Parse Washington election results for all elections before and including
    2007.

    """

    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1',
                                           delimiter=',')
            for row in reader:
                if self._skip_row(row):
                    continue
                else:
                    results.append(self._prep_county_results(row))
        try:
            RawResult.objects.insert(results)
        except errors.InvalidOperation:
            logger.error('\tNo raw results loaded')

    def _skip_row(self, row):
        return nr._is_match(
            row['officename']) not in self.target_offices

    def _build_contest_kwargs(self, row, primary_type):
        """
        Builds kwargs for specific contest

        """

        kwargs = {
            'office': nr._is_match(row['officename']),
            'primary_party': row['partycode'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        """
        Builds kwargs for specific candidate

        """

        full_name = [row['firstname'].strip(), row['lastname'].strip()]
        full_name = ' '.join(full_name).strip()

        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            'name_slug': slug,
        }

        return kwargs

    def _base_kwargs(self, row):
        """
        Builds a base set of kwargs for RawResult

        """

        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(
            row, kwargs['primary_type'])
        candidate_kwargs = self._build_candidate_kwargs(row)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

    def _prep_county_results(self, row):
        """
        In Washington our general results are reported by county instead
        of precinct, although precinct-level vote tallies are available.

        """

        kwargs = self._base_kwargs(row)
        county = str(row['jurisdiction'])
        kwargs.update({
            'reporting_level': row['reporting_level'],
            'jurisdiction': county,
            'party': row['partycode'].strip(),
            'votes': int(row['votes'].strip()),
            'county_ocd_id': self.mapping['ocd_id']
        })
        return RawResult(**kwargs)


class WALoaderPost2007(WABaseLoader):

    """
    Parse Washington election results for all elections after 2007.

    """

    def load(self):

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        results = []

        with self._file_handle as csvfile:
            no_district = 0
            reader = unicodecsv.DictReader(
                csvfile, encoding='latin-1', delimiter=',')
            self.header = [x.replace('"', '') for x in reader.fieldnames]
            for row in reader:
                if self._skip_row(row):
                    continue
                else:
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs['primary_party'] = row['Party'].strip()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    rr_kwargs.update({
                        'party': row['Party'].strip(),
                        'votes': int(row['Votes'].strip()),
                        'county_ocd_id': self.mapping['ocd_id'],
                    })
                    try:
                        rr_kwargs.update({
                            'district': '{0} {1}'.format(
                                nr._is_match(
                                        row[
                                            cm._normalize_contest(
                                                self.header)]), [
                                        int(s) for s in row[
                                            cm._normalize_contest(
                                                self.header)].strip()
                                        if s.isdigit()][0])})
                    except IndexError:
                        no_district += 1
                    results.append(RawResult(**rr_kwargs))
            if no_district != 0:
                logger.error(
                    'Index Error in WALoaderPost2007 while updating'
                    ' district: {0} lines had no district'
                    ' number'.format(no_district))

        """
        Many county files *only* have local races, such as schoolboard or
        fire chief races. Since openstates does not want these results,
        the entire files end up being skipped. To clarify the error message,
        we print our own if RawResult tries to insert nothing into mongodb

        """

        try:
            RawResult.objects.insert(results)
        except errors.InvalidOperation:
            logger.error('\tNo raw results loaded')

    def _skip_row(self, row):
        return nr._is_match(row['Race']) not in self.target_offices

    def _build_contest_kwargs(self, row):
        """
        if 'County' in self.reader.fieldnames:
            jurisdiction = row['County']
        else:
            jurisdiction = row['JurisdictionName']

        The above is the same as the code below, except a try/catch is quicker
        than an if/else statement.

        """

        try:
            jurisdiction = row['County'].strip()
        except KeyError:
            name_list = self.source.split('__')[-2:]
            jurisdiction = '{0} {1}'.format(
                name_list[0],
                name_list[1].split('.')[0])

        return {
            'office': row['Race'].strip(),
            'jurisdiction': jurisdiction
        }

    def _build_candidate_kwargs(self, row):
        full_name = row['Candidate'].strip()
        slug = slugify(full_name, substitute='-')
        return {
            'full_name': full_name,
            'name_slug': slug
        }


class WALoadExcel(WABaseLoader):

    def load(self):
        logger.error('\tCannot parse Excel files yet')
        pass
    """
        xlsfile = xlrd.open_workbook(self._xls_file_handle())

        sheets = self._get_sheets(xlsfile)
        print xlsfile
        for sheet in sheets:
            for i in xrange(sheet.nrows):
                row = [r for r in sheet.row_values(i) if not r == '']
                print str(row[0])

    def _get_sheets(self, xlsfile):
        sheets = xlsfile.sheets()
        return sheets
    """
