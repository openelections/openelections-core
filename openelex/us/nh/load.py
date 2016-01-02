import re
import xlrd

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
New Hampshire has a series of Excel files, most of which are county-based, but others span single or multiple
districts. Of the county-based files, Belknap County files can also contain statewide summaries. Some counties,
such as Grafton, display results in stacked tables. State legislative files contain no county information, just
precincts, so a crosswalk file is needed.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        election_year = int(election_id[3:7])
        generated_filename = mapping['generated_filename']

        if 'county' in mapping['ocd_id'] and 'governor' in generated_filename and 'belknap' not in generated_filename:
            loader = NHXlsCountyLoader()
            loader.run(mapping)
        elif 'belknap__governor' in generated_filename:
            #loader = NHBelknapCountyLoader()
            pass
        elif '__senate.xls' in generated_filename and 'belknap' not in generated_filename:
            loader = NHSenateCountyLoader()
            loader.run(mapping)
            #pass
        else:
            pass
            #loader = NHXlsLoader()
        #loader.run(mapping)


class NHBaseLoader(BaseLoader):
    datasource = Datasource()

    def _base_kwargs(self, row, office, district, candidate):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(office, district)
        candidate_kwargs = self._build_candidate_kwargs(candidate)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

    def _votes(self, val):
        """
        Returns cleaned version of votes or 0 if it's a non-numeric value.
        """
        if type(val) is str:
            if val.strip() == '':
                return 0

        try:
            return int(float(val))
        except ValueError:
            # Count'y convert value from string
            return 0

    def _get_office_and_primary_party(self, row):
        if "-" in row[1]:
            return [r.strip() for r in row[1].split('-')]
        else:
            return [row[1].strip(), None]

    def _build_contest_kwargs(self, office, district):
        return {
            'office': office,
            'district': district,
        }


class NHSenateCountyLoader(NHBaseLoader):
    """
    Loads New Hampshire county-specific XLS files containing precinct results for the following offices:
    Governor, President?
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []
        xlsfile = xlrd.open_workbook(self._xls_file_path)
        sheet = xlsfile.sheets()[0]
        office, primary_party = self._get_office_and_primary_party(sheet.row_values(1))
        district = None
        for i in xrange(2, sheet.nrows):
            row = [r for r in sheet.row_values(i)]
            if self._skip_row(row):
                continue
            if " County" in str(row[0]):
                county = row[0].split(' County')[0]
                precincts = [c for c in row[1:] if c.strip() != '']
            elif office in str(row[1]):
                continue
            else:
                candidate = row[0]
                for idx, precinct in enumerate(precincts):
                    results.append(self._prep_precinct_result(row, office, district, primary_party, precinct, candidate, county, row[idx+1]))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if row == []:
            return True
        elif str(row[0]).strip() == '':
            return True
        elif 'Correction received from clerk' in str(row[0]):
            return True
        elif any('State of New Hampshire' in str(c) for c in row):
            return True
        else:
            return False


    def _build_candidate_kwargs(self, candidate):
        if ", " in str(candidate):
            cand, party = candidate.split(", ")
            party = party.upper()
        else:
            cand = candidate
            party = None
        slug = slugify(cand, substitute='-')
        kwargs = {
            'full_name': cand,
            'name_slug': slug,
            'party': party
        }
        if 'Scatter' in cand:
            kwargs.update({'write_in': True})
        return kwargs

    def _prep_precinct_result(self, row, office, district, primary_party, precinct, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        if precinct.upper() == 'TOTALS':
            jurisdiction = None
            ocd_id = county_ocd_id
        else:
            jurisdiction = precinct
            ocd_id = "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct))
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': jurisdiction,
            'parent_jurisdiction': county,
            'ocd_id': ocd_id,
            'primary_party': primary_party,
            'votes': self._votes(votes)
        })
        if primary_party:
            kwargs.update({
                'party': primary_party
            })
        return RawResult(**kwargs)

    def _prep_county_result(self, row, office, district, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': county,
            'ocd_id': county_ocd_id,
            'party': candidate[1],
            'votes': self._votes(votes)
        })
        return RawResult(**kwargs)


class NHXlsCountyLoader(NHBaseLoader):
    """
    Loads New Hampshire county-specific XLS files containing precinct results for the following offices:
    Governor, President?
    """

    def load(self):
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []
        xlsfile = xlrd.open_workbook(self._xls_file_path)
        sheet = xlsfile.sheets()[0]
        office, primary_party = self._get_office_and_primary_party(sheet.row_values(1))
        district = None
        county = sheet.row_values(3)[0].split(' County')[0]
        candidates = sheet.row_values(3)[1:]
        start_row = 4
        for i in xrange(start_row, sheet.nrows):
            row = [r for r in sheet.row_values(i)]
            if self._skip_row(row):
                continue
            for idx, cand in enumerate(candidates):
                results.append(self._prep_precinct_result(row, office, district, primary_party, cand, county, row[idx+1]))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if row == []:
            return True
        elif row[0].strip() == '':
            return True
        elif 'Correction received from clerk' in row[0]:
            return True
        else:
            return False

    def _build_candidate_kwargs(self, candidate):
        if ", " in candidate:
            cand, party = candidate.split(", ")
            party = party.upper()
        else:
            cand = candidate
            party = None
        slug = slugify(cand, substitute='-')
        kwargs = {
            'full_name': cand,
            'name_slug': slug,
            'party': party
        }
        if 'Scatter' in cand:
            kwargs.update({'write_in': True})
        return kwargs

    def _prep_precinct_result(self, row, office, district, primary_party, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        precinct = str(row[0]).strip()
        if precinct.upper() == 'TOTALS':
            jurisdiction = None
            ocd_id = county_ocd_id
        else:
            jurisdiction = precinct
            ocd_id = "{}/precinct:{}".format(county_ocd_id, ocd_type_id(precinct))
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': jurisdiction,
            'parent_jurisdiction': county,
            'ocd_id': ocd_id,
            'primary_party': primary_party,
            'votes': self._votes(votes)
        })
        if primary_party:
            kwargs.update({
                'party': primary_party
            })
        return RawResult(**kwargs)

    def _prep_county_result(self, row, office, district, candidate, county, votes):
        kwargs = self._base_kwargs(row, office, district, candidate)
        county_ocd_id = [c for c in self.datasource._jurisdictions() if c['county'].upper() == county.upper()][0]['ocd_id']
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': county,
            'ocd_id': county_ocd_id,
            'party': candidate[1],
            'votes': self._votes(votes)
        })
        return RawResult(**kwargs)
