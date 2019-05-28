# -*- coding: utf-8 -*-

from builtins import next
from builtins import object
from os.path import exists, join
import re
import unicodecsv as csv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Wisconsin elections have pre-processed CSV results files for elections beginning in 2002.
These files contain ward-level results. The CSV versions of those are contained in the
https://github.com/openelections/openelections-data-wi repository.
"""

class LoadResults(object):
    """
    Entry point for data loading.
    Determines appropriate loader for file and triggers load process.
    """

    def run(self, mapping):
        election_id = mapping['generated_filename']
        loader = WIPrecinctLoader()
        loader.run(mapping)

class WIBaseLoader(BaseLoader):
    datasource = Datasource()

class WIPrecinctLoader(WIBaseLoader):
    """
    Loads Wisconsin precinct results for 2002-2017.

    Format:
    Wisconsin has Excel files that have been pre-processed to CSV for elections after 2002.
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []
        if not(exists(join(self.cache.abspath, self.source))):
            return
        with self._file_handle as csvfile:
            reader = csv.DictReader(csvfile, encoding='utf8')
            next(reader, None)
            for row in reader:
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                rr_kwargs.update(self._build_jurisdiction_kwargs(row))
                rr_kwargs.update({
                    'primary_party': row['party'].strip(),
                    'party': row['party'].strip(),
                    'votes': int(float(row['votes']))
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _build_jurisdiction_kwargs(self, row):
        jurisdiction = row['ward'].strip()
        county_map = self.datasource._ocd_id_for_county_map()
        county_ocd_id = county_map[row['county'].strip().upper()]
        return {
            'jurisdiction': jurisdiction,
            'parent_jurisdiction': row['county'],
            'ocd_id': "{}/precinct:{}".format(county_ocd_id, ocd_type_id(jurisdiction)),
        }

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        original_name = row['candidate'].strip()
        clean_name = original_name.replace(' (Write-In)', '').replace(' (Write In)', '')
        write_in = (original_name != clean_name)
        return {
            'full_name': clean_name,
            'write_in': write_in
        }
