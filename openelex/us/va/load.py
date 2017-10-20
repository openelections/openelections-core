from builtins import str
from builtins import object
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.insertbuffer import BulkInsertBuffer
from openelex.lib.text import ocd_type_id
from .datasource import Datasource

"""
Virginia elections have CSV results files for elections beginning in 2005. These files contain precinct-level data
for all of the state's counties and independent cities, and includes all contests. Special election results are
contained in election-specific files. Prior to Nov. 2005 general, files are contained in race-specific CSV files at
http://historical.elections.virginia.gov/elections.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        loader = CSVLoader()
        loader.run(mapping)


class VABaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'President and Vice President',
        'President and Vice President - 2001 CD Lines',
        'President - 2001 CD Lines',
        'United States Senate',
        'United States Senate - 2001 CD Lines',
        'Governor',
        'Governor - 2001 CD Lines',
        'Lieutenant Governor',
        'Lieutenant Governor - 2001 CD Lines',
        'Attorney General',
        'Attorney General - 2001 CD Lines',
        'Member House of Representatives',
        'Member House of Representatives - 2001 CD Lines',
        'Member Senate of Virginia',
        'Member Senate of Virginia - 2001 SD Lines',
        'Member House of Delegates',
        'Member, House of Delegates',
        'Member House of Delegates - 2001 LD Lines',
        'State Senate',
        'State House',
    ])

    district_offices = set([
        'Member House of Representatives',
        'Member House of Representatives - 2001 CD Lines'
        'Member Senate of Virginia',
        'Member Senate of Virginia - 2001 SD Lines',
        'Member House of Delegates',
        'Member, House of Delegates',
        'Member House of Delegates - 2001 LD Lines',
        'State Senate',
        'State House',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

class CSVLoader(VABaseLoader):
    """
    Loads Virginia primary, special and general election results for 2005-2015.
    """

    offices = {
        'President and Vice President': 'President',
        'President and Vice President - 2001 CD Lines': 'President',
        'President - 2001 CD Lines': 'President',
        'United States Senate': 'U.S. Senate',
        'United States Senate - 2001 CD Lines': 'U.S. Senate',
        'Governor': 'Governor',
        'Governor - 2001 CD Lines': 'Governor',
        'Lieutenant Governor': 'Lieutenant Governor',
        'Lieutenant Governor - 2001 CD Lines': 'Lieutenant Governor',
        'Attorney General': 'Attorney General',
        'Attorney General - 2001 CD Lines': 'Attorney General',
        'Member House of Representatives': 'U.S. House',
        'Member House of Representatives - 2001 CD Lines': 'U.S. House',
        'Member Senate of Virginia': 'State Senate',
        'Member Senate of Virginia - 2001 SD Lines': 'State Senate',
        'Member House of Delegates': 'State House',
        'Member House of Delegates - 2001 LD Lines': 'State House',
        'Member, House of Delegates': 'State House'
    }

    def load(self):
        headers = [
            'CandidateUid',
            'FirstName',
            'MiddleName',
            'LastName',
            'Suffix',
            'TOTAL_VOTES',
            'Party',
            'WriteInVote',
            'LocalityUid',
            'LocalityCode',
            'LocalityName',
            'PrecinctUid',
            'PrecinctName',
            'DistrictUid',
            'DistrictType',
            'DistrictName',
            'OfficeUid',
            'OfficeTitle',
            'ElectionUid',
            'ElectionType',
            'ElectionDate',
            'ElectionName'
        ]

        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = BulkInsertBuffer(RawResult)

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                if 'primary' in self.mapping['election']:
                    rr_kwargs['primary_party'] = row['Party'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                rr_kwargs.update(self._build_write_in_kwargs(row))
                rr_kwargs.update(self._build_total_votes(row))
                parent_jurisdiction = [c for c in self.datasource._jurisdictions() if int(c['fips']) == int(row['LocalityCode'])][0]
                if row['PrecinctUid'].strip() == '':
                    ocd_id = parent_jurisdiction['ocd_id']
                else:
                    ocd_id = "{}/precinct:{}".format(parent_jurisdiction['ocd_id'], ocd_type_id(str(row['PrecinctName'])))
                rr_kwargs.update({
                    'party': row['Party'].strip(),
                    'jurisdiction': str(row['PrecinctName']),
                    'parent_jurisdiction': parent_jurisdiction['name'],
                    'ocd_id': ocd_id
                })
                results.append(RawResult(**rr_kwargs))

        results.flush()

    def _skip_row(self, row):
        return row['OfficeTitle'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        office = self.offices[row['OfficeTitle']]
        if row['OfficeTitle'] not in self.district_offices:
            district = None
        else:
            district = row['DistrictName']
        return {
            'office': office,
            'district': district,
        }

    def _build_total_votes(self, row):
        if row['TOTAL_VOTES'].strip() == '':
            votes = None
        else:
            votes = int(row['TOTAL_VOTES'])
        return {
            'votes': votes
        }

    def _build_write_in_kwargs(self, row):
        if row['WriteInVote'] == '1':
            write_in = True
        else:
            write_in = False

        return {
            'write_in': write_in
        }

    def _build_candidate_kwargs(self, row):
        return {
            'given_name': row['FirstName'],
            'family_name': row['LastName'],
            'additional_name': row['MiddleName'],
            'suffix': row['Suffix']
        }
