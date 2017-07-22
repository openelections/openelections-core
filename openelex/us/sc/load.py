import clarify

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.insertbuffer import BulkInsertBuffer
from openelex.lib.text import ocd_type_id
from .datasource import Datasource

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if 'special' in election_id:
            loader = SpecialLoader()
        else:
            loader = XMLCountyLoader()
        loader.run(mapping)


class SCBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'Governor',
        'Lieutenant Governor',
        'State Treasurer',
        'Attorney General',
        'Comptroller General',
        'State Superintendent of Education',
        'Adjutant General',
        'Commissioner of Agriculture',
        'U.S. Senate',
        'U.S. Senate (Unexpired Term)',
        'U.S. House of Representatives',
        'State Senate',
        'State House of Representatives',
        'State Representative'
    ])

    district_offices = set([
        'U.S. House of Representatives',
        'State Senate',
        'State House of Representatives',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

class XMLCountyLoader(SCBaseLoader):
    """
    Loads Clarity-based XML county-level results for primary and general elections.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        p = clarify.Parser()

        p.parse(self._file_handle)
        for result in p.results:
            if self._skip_row(result.contest):
                continue
            if row['county'].strip() == '':
                total_votes = int(row['votes'].strip())
            else:
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = row['county'].strip()
                rr_kwargs.update({
                    'jurisdiction': jurisdiction,
                    'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                        ocd_type_id(jurisdiction)),
                    'office': row['office'].strip(),
                    'district': row['district'].strip(),
                    'votes': int(row['votes'].strip())
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, contest):
        return contest.strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip(),
            'party': row['party'].strip()
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip()
        }
