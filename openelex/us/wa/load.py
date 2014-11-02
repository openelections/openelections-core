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
from openelex.lib.text import ocd_type_id
from .datasource import Datasource

"""
Washington state elections have CSV and XLSX result files.
Results from < 2007 have a different format than those <= 2008.
Actually, most every file has a different format.

NOTES:

1.) Loader uses a few normalizing functions that normalize parts of the data.
    In particular, we use some normalize_* to normalize the headers of different
    files whose headers are generally the same, but differ in the wording.

    For example, some files will have all the same fields, but name them
    slightly differently. In one file, the column that holds the candidate's
    name might be "CANDIDATE_FULL_NAME", while another might be
    "candidate name". Because of this, we use regex to test the header row
    to find the correct field.

    normalize_* also takes the race data and then matches it against
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
        bad_filenames[] holds the list of files who have content that's
        hard to use (e.g. an .xls file with 10 sheets).

        The edge cases will be taken care of later. The cases where there is
        zero actual usable data will have to be rectified outside of the
        loader module.

        """

        bad_filenames = [
            # The below are Excel (.xls) files that have results spread across
            # multiple worksheets and in different structures from each other

            '20070821__wa__primary.xls',
            '20070821__wa__primary__county.xls',
            '20080219__wa__primary__adams__precinct.xls',
            '20080219__wa__primary__benton__precinct.xls',
            '20080219__wa__primary__congressional_district_state_legislative.xls',
            '20080219__wa__primary__douglas__precinct.xls',
            '20080219__wa__primary__kitsap__precinct.xls',
            '20080819__wa__primary__kitsap__precinct.xls',
            '20080819__wa__primary__pierce__precinct.xls',
            '20081104__wa__general__congressional_district.xls',
            '20081104__wa__general__adams__precinct.xls',
            '20091103__wa__general__clark__precinct.xls',
            '20081104__wa__general__franklin__precinct.xls',
            '20081104__wa__general__kittitas__precinct.xls',
            '20081104__wa__general__kitsap__precinct.xls',
            '20081104__wa__general__pierce__precinct.xls',
            '20081104__wa__general__precinct.xls',
            '20081104__wa__general__state_legislative.xls',
            '20091103__wa__general__kitsap__precinct.xls',
            '20091103__wa__general__pierce__precinct.xls',
            '20101102__wa__general__kittitas___precinct.xls',
            '20101102__wa__general__san_juan___precinct.xls',
            '20100817__wa__primary__state_legislative.xls',
            '20100817__wa__primary__congressional_district.xls',
            '20111108__wa__general__clark___precinct.xlsx',
            '20111108__wa__general__spokane___precinct.xlsx',
            '20120807__wa__primary__congressional_district.xls',
            '20120807__wa__primary__state_legislative.xls',
            '20121106__wa__general__congressional_district.xls',
            '20121106__wa__general__state_legislative.xls',
        ]

        """
        Could try using `generated_filename.split(.)[-1]` instead of
        os.path.splitext(election)[-1], since all filenames are
        standardized. This would, of course, break if the file path includes
        a full stop (period).

        """

        # If files are 'bad', skip them
        if any(x in generated_filename for x in bad_filenames):
            loader = SkipLoader()

        # If files are .xls(x), use the correct loader
        elif os.path.splitext(
                generated_filename)[-1].lower() in ('.xls', '.xlsx'):
            loader = WALoaderExcel()

        elif os.path.splitext(generated_filename)[-1].lower() == '.txt':

            """
            We run into issues where King County provides > 1 million line
            .txt files that break my machine's memory. We definitely need to
            refactor, but for the moment we'll pass over said files.

            """

            logger.info(
                'Cannot do anything with {0}'.format(generated_filename))
            loader = SkipLoader()

        elif 'precinct' in generated_filename:
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

        elif os.path.splitext(
                generated_filename)[-1].lower() in ('.csv', '.txt'):
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
                '\tUnsupported file type ({0})'
                .format('UnboundLocalError'))
        except IOError:
            logger.error(
                '\tFile "{0}" does not exist'
                .format(generated_filename))
        except unicodecsv.Error:
            logger.error(
                '\tUnsupported file type "({0})"'
                .format('unicodecsv.Error'))
        except errors.InvalidOperation:
            logger.error('\tNo raw results loaded')


class OCDMixin(object):

    """
    Borrowed from md/loader.py
    Generates ocd_id

    """

    def _get_ocd_id(self, jurisdiction, precinct=False):
        if precinct:
            return "{}/county:{}/precinct:{}".format(
                self.mapping['ocd_id'],
                ocd_type_id(jurisdiction),
                ocd_type_id(precinct))
        elif 'county' in self.mapping['ocd_id']:
            return "{}".format(self.mapping['ocd_id'])
        else:
            return "{}/county:{}".format(
                self.mapping['ocd_id'],
                ocd_type_id(jurisdiction))


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
        'Commissioner of Public Lands',
    ])

    district_offices = {
        #'U.S. Senator': 'Congressional District',
        'U.S. Representative': 'Congressional District',
        'State Senator': 'Legislative District',
        'State Representative': 'Legislative District',
    }

    def _skip_row(self, row):
        """
        Should this row be skipped?
        This should be implemented in subclasses.

        """
        return False

"""
New methods to normalize headers should follow this structure:

    def *_(header):


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


def normalize_party(header):
    """
    Regex examples:

    party = true
    party_code = true
    party code = true

    """

    regex = re.compile(
        r'.*(\bparty\b|party.*code|candidate(_|\s+)party(_|\s)id).*',
        re.IGNORECASE)

    """
    `return filter(lambda x: regex.search(x), header)[0]`
    does the same as the below list comprehension

    """

    return filter(lambda x: regex.search(x), header)[0]


def normalize_candidate(header):
    """
    Regex examples:

    candidate = true
    candidate_name = true
    candidate_id = false
    candidate_full_name = true

    """

    regex = re.compile(
        r'.*(ballot\sname|candidate.*(name|title)|candidate\b).*',
        re.IGNORECASE)

    return filter(lambda x: regex.search(x), header)[0]


def normalize_contest(header):
    """
    Regex examples:

    contest = true
    race = true
    contest_name = true
    contest_id = false

    """

    regex = re.compile(
        r'.*(officeposition|\bcontest\b|race\b|race(_|\s)(title|name)|(contest.*(title|name))).*',
        re.IGNORECASE)

    return filter(lambda x: regex.search(x), header)[0]


def normalize_precinct(header):
    """
    Regex examples:

    precinct = true
    precinct_name = true
    precinct name = true

    """

    regex = re.compile(r'.*(precinct|precinct.*name).*', re.IGNORECASE)

    return filter(lambda x: regex.search(x), header)[0]


def normalize_votes(header):
    """
    Regex examples:

    number of votes for = true
    votes = true
    count = true
    total number of votes = false

    """

    regex = re.compile(
        r'.*(.*vote.*for|\bvote|\bcount\b|total_votes|total.*votes).*',
        re.IGNORECASE)

    return filter(lambda x: regex.search(x), header)[0]


def normalize_index(header, method):
    """
    Equivalent to:

    self.votes_index = self.header.index(
        ''.join(votes(self.header)))
    """

    return header.index(''.join(method(header)))


def normalize_district(header, office, row):
    """
    Example of what we had before:

    'district': '{0} {1}'.format(
        self.district_offices[normalize_races(sh_val)],
        "".join(map(str, [int(s) for s in sh_val.strip() if s.isdigit()][:2])
        ))})

    normalize_district now provides a more standardized and clean API than
    was the case with the mess before.
    """

    norm_office = normalize_races(office)
    dist_str = "".join(
        map(str, [int(s) for s in office.strip() if s.isdigit()][:2]))

    bth_regex = re.compile(r'((leg|con).*dis.*)', re.IGNORECASE)
    leg_regex = re.compile(r'leg.*dis.*', re.IGNORECASE)
    con_regex = re.compile(r'con.*dis.*', re.IGNORECASE)

    if not row:
        row = {}

    try:
        row[filter(lambda x: bth_regex.search(x), header)[0]]
        if norm_office is 'U.S. Representative':
            dist = row[filter(lambda x: leg_regex.search(x), header)[0]]
            return dist
        if norm_office in ('State Representative', 'State Senate'):
            dist = row[filter(lambda x: con_regex.search(x), header)[0]]
            return dist
    except IndexError:
        if dist_str is "":
            return None
        if int(dist_str) > 49:
            dist_str = dist_str[:1]
        return dist_str


def normalize_races(string):
    """
    Normalizes races per 'target_offices'

    Although we should not provide 'N/A' in places where we don't have
    valid data (e.g. if no party is stated, we simply don't provide the
    party value instead of providing 'N/A' or a blank value), returning
    'N/A' here will result in us skipping the row, since this class is
    and only should be used *only* in the `self._skip_row` methods.

    Returning anything other than one of the values in `target_offices`
    will result in the row being skipped. Since 'N/A' isn't in
    `target_offices`, we're fine.

    """

    general_filter_regex = re.compile(r'(countywide|initiative|county of|city of|port|director|council|school|mayor)', re.IGNORECASE)
    presidential_regex = re.compile('president', re.IGNORECASE)
    senate_regex = re.compile(r'(senate|senator)', re.IGNORECASE)
    house_regex = re.compile(r'(house|representative)', re.IGNORECASE)
    governor_regex = re.compile('governor', re.IGNORECASE)
    treasurer_regex = re.compile('treasurer', re.IGNORECASE)
    auditor_regex = re.compile('auditor', re.IGNORECASE)
    sos_regex = re.compile('secretary', re.IGNORECASE)
    lt_gov_regex = re.compile(r'(lt|Lieutenant)', re.IGNORECASE)
    ospi_regex = re.compile(
        'superintendent of public instruction',
        re.IGNORECASE)
    ag_regex = re.compile('attorney general', re.IGNORECASE)
    wcpl_regex = re.compile('commissioner of public lands', re.IGNORECASE)
    local_regex = re.compile(
        r'(^State\b|Washington|Washington\s+State|Local|Legislative District)',
        re.IGNORECASE)
    national_regex = re.compile(
        r'(U\.S\.|\bUS\b|Congressional|National|United\s+States|U\.\s+S\.\s+)',
        re.IGNORECASE)

    """
    The following chained if statements are ordered by the most frequent
    occurrences. As of August 26th, 2014 these are the results from
    running `egrep -rohi 'regex' . | wc -l`

    I've placed Lt. Governor's regex ahead of Governor's in order to
    be able to get the Lt. Governor's values and keep a simplified regex.

    These aren't exact, but give are a rough assessment of the number
    of occurrences.

    National:  935375
    Local:     953031

    *House:    417020
    Governor:  319836
    CPL:       344795
    *Senate:   186247
    Lt. Gov.:  161537
    SPI:       128783
    SoS:       122404
    Auditor:   103920
    AG:        85059
    President: 75183

    """

    if re.search(general_filter_regex, string):
        return 'N/A'
    elif re.search(house_regex, string):
        if re.search(national_regex, string):
            return 'U.S. Representative'
        elif re.search(local_regex, string):
            return 'State Representative'
        else:
            return 'N/A'
    elif re.search(lt_gov_regex, string):
        return 'Lt. Governor'
    elif re.search(governor_regex, string):
        return 'Governor'
    elif re.search(wcpl_regex, string):
        return 'Commissioner of Public Lands'
    elif re.search(senate_regex, string):
        if re.search(national_regex, string):
            return 'U.S. Senator'
        elif re.search(local_regex, string):
            return 'State Senator'
        else:
            return 'N/A'
    elif re.search(ospi_regex, string):
        return 'Superintendent of Public Instruction'
    elif re.search(sos_regex, string):
        return 'Secretary of State'
    elif re.search(treasurer_regex, string):
        return 'Treasurer'
    elif re.search(auditor_regex, string):
        return 'Auditor'
    elif re.search(ag_regex, string):
        return 'Attorney General'
    elif re.search(presidential_regex, string):
        return 'President'
    else:
        return 'N/A'


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
        logger.error('\tNothing we can do with {0}'.format(self.source))
        pass


class WALoaderPrecincts(OCDMixin, WABaseLoader):

    """
    Parse Washington election results for all precinct files.
    This class uses the Normalize class to normalize the column
    headers.

    """

    header = ''
    votes_index = ''
    party_index = ''
    contest_index = ''
    candidate_index = ''
    precinct_index = ''
    jurisdiction = ''

    def load(self):

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        results = []

        with self._file_handle as csvfile:
            party_flag = 0
            district_flag = 0
            reader = unicodecsv.DictReader(
                csvfile, encoding='latin-1', delimiter=',')

            # Declare column indices before the loop so we aren't making
            # a method call for each line in the file

            self.header = [x.replace('"', '') for x in reader.fieldnames]
            self.votes_index = normalize_votes(self.header)
            self.contest_index = normalize_contest(self.header)
            self.candidate_index = normalize_candidate(
                self.header)
            self.precinct_index = normalize_precinct(
                self.header)
            try:
                self.party_index = normalize_party(self.header)
            except IndexError:
                pass

            for row in reader:
                if self._skip_row(row):
                    continue
                else:
                    self.jurisdiction = row[self.precinct_index].strip()
                    votes = int(row[self.votes_index].strip())
                    rr_kwargs = self._common_kwargs.copy()
                    rr_kwargs.update(self._build_contest_kwargs(row))
                    rr_kwargs.update(self._build_candidate_kwargs(row))
                    rr_kwargs.update({
                        'reporting_level': 'precinct',
                        'votes': votes,
                        'ocd_id': "{}".format(self._get_ocd_id(
                            self.jurisdiction,
                            precinct=row[self.precinct_index]))
                    })
                    try:
                        rr_kwargs.update({
                            'party': row[self.party_index].strip()
                        })
                    except (IndexError, KeyError):
                        party_flag = 1
                    try:
                        rr_kwargs.update(
                            {'district': normalize_district(self.header, row[self.contest_index], row)})
                    except KeyError:
                        district_flag = 1
                    results.append(RawResult(**rr_kwargs))
            if 0 is not party_flag:
                logger.info('Some rows did not contain party info.')
            if 0 is not district_flag:
                logger.info('Some rows did not contain district info.')

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
        return normalize_races(
            row[self.contest_index]) not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'office': row[self.contest_index].strip(),
            'jurisdiction': self.jurisdiction,
        }

    def _build_candidate_kwargs(self, row):
        full_name = row[self.candidate_index].strip()

        return {
            'full_name': full_name
        }

"""
I don't know why this class is set up different from the rest.
I should fix this.

"""


class WALoaderPre2007(OCDMixin, WABaseLoader):

    """
    Parse Washington election results for all elections before 2007.

    """

    # Declare column indices before the loop so we aren't making
    # a method call for each line in the file

    header = ''
    contest_index = ''

    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1',
                                           delimiter=',')
            self.header = [x.replace('"', '') for x in reader.fieldnames]

            try:
                self.contest_index = normalize_contest(self.header)
            except IndexError:
                pass

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
        return normalize_races(row['officename']) not in self.target_offices

    def _build_contest_kwargs(self, row, primary_type):
        """
        Builds kwargs for specific contest

        """

        kwargs = {
            'office': row['officename'],
            'primary_party': row['partycode'].strip()
        }
        return kwargs

    def _build_candidate_kwargs(self, row):
        """
        Builds kwargs for specific candidate

        """

        family_name = row['lastname'].strip()
        given_name = row['firstname'].strip()

        kwargs = {
            'family_name': family_name,
            'given_name': given_name
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
            'ocd_id': "{}".format(self._get_ocd_id(county))
        })
        try:
            kwargs.update({
                'district': normalize_district(self.header, row[self.contest_index], row)
            })
        except KeyError:
            pass
        return RawResult(**kwargs)


class WALoaderPost2007(OCDMixin, WABaseLoader):

    """
    Parse Washington election results for all elections after and including 2007.

    """

    header = ''
    contest_index = ''

    def load(self):

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        results = []

        with self._file_handle as csvfile:
            district_flag = 0
            reader = unicodecsv.DictReader(
                csvfile, encoding='latin-1', delimiter=',')
            self.header = [x.replace('"', '') for x in reader.fieldnames]
            self.contest_index = normalize_contest(self.header)
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
                        'ocd_id': "{}".format(self._get_ocd_id(rr_kwargs['jurisdiction'])),
                    })
                    try:
                        rr_kwargs.update(
                            {'district': normalize_district(self.header, row[self.contest_index], row)})
                    except KeyError:
                        district_flag = 1
                    results.append(RawResult(**rr_kwargs))
            if 0 is not district_flag:
                logger.info('Some rows did not contain district info.')

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
        return normalize_races(row['Race']) not in self.target_offices

    def _build_contest_kwargs(self, row):
        """
        if 'County' in self.reader.fieldnames:
            jurisdiction = row['County']
        else:
            jurisdiction = row['JurisdictionName']

        The above is the same as the code below, except a try/catch is quicker
        than an if/else statement. Plus, Python is EAFP, not LBYL.

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
        return {
            'full_name': full_name
        }


class WALoaderExcel(OCDMixin, WABaseLoader):

    """ Load Excel (.xls/.xlsx) results """

    header = ''
    votes_index = ''
    party_index = ''
    contest_index = ''
    candidate_index = ''
    precinct_index = ''
    jurisdiction_index = ''

    def load(self):
        xlsfile = xlrd.open_workbook(self._xls_file_path)
        self._common_kwargs = self._build_common_election_kwargs()

        # Set the correct reporting level based on file name
        if 'precinct' in self.mapping['generated_filename']:
            reporting_level = 'precinct'
        else:
            reporting_level = 'county'

        self._common_kwargs['reporting_level'] = reporting_level
        results = []
        sheet = xlsfile.sheet_by_index(0)

        """
        I ran into an issue where RawResult wasn't loading any results for my
        .xls files. I hypothesized that the _skip_row method was, for whatever
        reason, skipping all the results. I was correct, and found out that
        the indices of an Excel sheet (through the xlrd module) need to be
        integers, not string. My normalzing class returns strings, thus
        causing _skip_row to always return false as xlrd couldn't do
        anything with a string.

        self.header is a list, and so I run the list through my normalzing
        class which returns a list with one value (the column we want). I
        turn that list value into a string and find the index of that string
        within the header list.

        That returns the correct integer value for the column which holds the
        contest name.

        """

        self.header = sheet.row_values(0)
        self.votes_index = normalize_index(
            self.header,
            normalize_votes)
        self.contest_index = normalize_index(
            self.header,
            normalize_contest)
        self.candidate_index = normalize_index(
            self.header,
            normalize_candidate)
        self.precinct_index = normalize_index(
            self.header,
            normalize_precinct)
        self.jurisdiction_index = normalize_index(
            self.header,
            normalize_precinct)
        try:
            self.party_index = normalize_index(
                self.header,
                normalize_precinct)
        except IndexError:
            pass

        for row in xrange(sheet.nrows):
            if self._skip_row(row, sheet):
                continue
            else:
                votes = int(sheet.cell(rowx=row, colx=self.votes_index).value)
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_candidate_kwargs(row, sheet))
                rr_kwargs.update(self._build_contest_kwargs(row, sheet))
                rr_kwargs.update({
                    'votes': votes,
                    'ocd_id': "{}".format(self._get_ocd_id(rr_kwargs['jurisdiction']))
                })
                # Get party
                try:
                    party = str(sheet.cell(
                            rowx=row,
                            colx=self.party_index).value).strip()
                    rr_kwargs.update({
                        'party': party
                    })
                except TypeError:
                    """
                    Should this be implemented?
                    Would need to extract the error message from the loop
                    to avoid potentially printing the message over 1,000 times

                    """
                    # logger.info('No party')
                    pass
                try:
                    sh_val = sheet.cell(
                        rowx=row,
                        colx=self.contest_index).value
                    rr_kwargs.update(
                        {'district': '{}'.format(normalize_district(self.header, sh_val, row=False))})
                except KeyError:
                    pass
        RawResult.objects.insert(results)

    def _skip_row(self, row, sheet):
        return normalize_races(
            sheet.cell(
                rowx=row,
                colx=self.contest_index).value.strip()) not in self.target_offices

    def _build_contest_kwargs(self, row, sheet):
        """
        Coerce the jurisdiction into a string because some precinct
        jurisdictions are numbers and getting that value from an Excel file
        returns a float. You can't .strip() a float.

        """

        jurisdiction = str(
            sheet.cell(
                rowx=row,
                colx=self.jurisdiction_index).value).strip()

        return {
            'office': sheet.cell(
                rowx=row,
                colx=self.contest_index).value.strip(),
            'jurisdiction': jurisdiction
        }

    def _build_candidate_kwargs(self, row, sheet):
        full_name = sheet.cell(rowx=row, colx=self.candidate_index).value

        return {
            'full_name': full_name
        }
