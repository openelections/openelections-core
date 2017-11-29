"""Converts csv file data into RawResults for Indiana election results.

Indiana elections have pre-processed CSV results files for primary and
general elections beginning in 2002. These files contain county-level
and, where available, precinct-level elections data for each of the
state's counties.  The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-in repository.
"""
from __future__ import print_function

from builtins import object
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id
from .datasource import Datasource


class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.
    """

    def run(self, mapping):
        election_id = mapping['pre_processed_url']
        if 'precinct' in election_id and 'special' not in election_id:
            loader = INPrecinctLoader()
        else:
            raise RuntimeError(
                'Cannot process election mapping {}'.format(mapping))
        loader.run(mapping)


class INPrecinctLoader(BaseLoader):
    """Loads Indiana precinct-level results for the 2014 general election.

    Indiana has PDF files that have been converted to CSV files for
    precinct-level election data from 2012-2016.
    """
    datasource = Datasource()

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'

        # Store result instances for bulk loading
        results = []
        num_skipped = 0
        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            for row in reader:
                if self._skip_row(row):
                    num_skipped += 1
                    continue

                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                # The 'votes' column gets screwed up a lot, so handle it
                # by additionally printing debug information.
                try:
                    rr_kwargs.update({'votes': int(row['votes'])})
                except ValueError as e:
                    print('Bad votes in row {}'.format(row))
                    raise e

                county = row['county'].strip()
                county_ocd_id = self._get_county_ocd_id(county)
                precinct = row['precinct'].strip()
                if precinct:
                  precinct_ocd_id = "{}/precinct:{}".format(
                      county_ocd_id, ocd_type_id(precinct)),
                  rr_kwargs.update({
                      'ocd_id': precinct_ocd_id,
                      'jurisdiction': precinct,
                      'parent_jurisdiction': county,
                  })
                else:
                  rr_kwargs.update({
                      'ocd_id': county_ocd_id,
                      'jurisdiction': county,
                      'parent_jurisdiction': 'ocd-division/country:us/state:in',
                  })
                results.append(RawResult(**rr_kwargs))

        print('\tInserting {} results (skipped {} rows)'.format(len(results),
                                                                num_skipped))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if not row['county'].strip():  # Some extraneous data
            return True
        elif row['votes'].strip() == '':  # Unreported data
            return True
        return False

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip() or None,
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip(),
            'party': row['party'].strip(),
        }

    def _get_county_ocd_id(self, county):
        for j in self.datasource.jurisdiction_mappings():
            if j['county'].upper() == county.upper():
                return  j['ocd_id']
        counties = [j['county']
                    for j in self.datasource.jurisdiction_mappings()]
        raise RuntimeError('Did not find county ocd id for {} in {}'.format(
            county, sorted(counties)))
