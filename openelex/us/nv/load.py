import re
import csv
from lxml import etree
import unicodecsv
from itertools import islice

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Nevada elections have CSV results files for county results from 2000-2012, and
XML files for 2012 and 2014. Precinct-level CSV files are available for elections from 2004-2012.
County-level CSV versions are contained in the https://github.com/openelections/openelections-data-nv
repository.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['generated_filename']
        if 'precinct' in election_id:
            loader = NVPrecinctLoader()
        elif mapping['raw_url'] != '':
            loader = NVXmlLoader()
        else:
            loader = NVCountyLoader()
        loader.run(mapping)


class NVBaseLoader(BaseLoader):
    datasource = Datasource()

    target_offices = set([
        'PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES',
        'PRESIDENT',
        'UNITED STATES SENATOR',
        'U.S. REPRESENTATIVE IN CONGRESS',
        'REPRESENTATIVE IN CONGRESS',
        'GOVERNOR',
        'LIEUTENANT GOVERNOR',
        'SECRETARY OF STATE',
        'STATE TREASURER',
        'STATE CONTROLLER',
        'ATTORNEY GENERAL',
        'STATE SENATE',
        'CENTRAL NEVADA SENATORIAL',
        'STATE ASSEMBLY',
    ])

    district_offices = set([
        'U.S. REPRESENTATIVE IN CONGRESS',
        'STATE SENATE',
        'STATE ASSEMBLY',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False

    def _build_contest_kwargs(self, row):
        if 'primary' in self.mapping['election']:
            office = row['office'].split('(')[0].split(', ')[0].strip()
            primary_party = row['office'].strip().split('(')[1].split(')')[0]
            if 'DISTRICT' in row['office'].upper():
                # 2002-4 primary district has no comma
                if '2004' in self.mapping['election'] or '2002' in self.mapping['election']:
                    if row['office'].strip() == 'CENTRAL NEVADA SENATORIAL DISTRICT (Republican)':
                        office = 'STATE SENATE'
                        district = 'CENTRAL NEVADA'
                        primary_party = 'Republican'
                    else:
                        office = office.split(' DISTRICT')[0]
                        district = row['office'].split('DISTRICT ')[1].split(' (')[0].strip()
                else:
                    district = row['office'].split(', ')[1].split(' (')[0].strip()
            else:
                district = None
        else:
            primary_party = None
            if 'DISTRICT' in row['office'].upper():
                district = row['office'].split(', ')[1].strip()
                office = row['office'].split(', ')[0].strip()
            else:
                district = None
                office = row['office'].strip()
        return {
            'office': office,
            'district': district,
            'primary_party': primary_party,
            'party': primary_party
        }

    def _build_candidate_kwargs(self, row):
        return {
            'full_name': row['candidate'].strip()
        }

class NVPrecinctLoader(NVBaseLoader):
    """
    Loads Nevada results for 2004-2012.

    Format:

    Nevada has CSV files for elections after 2004 primary.
    Header rows are the fourth row of the file.
    """

    def load(self):
        # use first row as headers, not pre-canned list
        # need to use OCD_ID from jurisdiction in mapping
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'precinct'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1', fieldnames=("Jurisdiction", "Precinct", "office", "candidate", "Votes"))
            next(reader, None)
            next(reader, None)
            next(reader, None)
            next(reader, None)
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                ocd_id = [c for c in self.datasource._jurisdictions() if c['jurisdiction'] == row['Jurisdiction']][0]['ocd_id']
                jurisdiction = row['Precinct'].strip()
                if row['Votes'].strip() == '*':
                    votes = 'N/A'
                else:
                    votes = int(row['Votes'].replace(',','').strip())
                rr_kwargs.update({
                    'jurisdiction': jurisdiction,
                    'parent_jurisdiction': row['Jurisdiction'],
                    'ocd_id': "{}/precinct:{}".format(self.mapping['ocd_id'], ocd_type_id(row['Precinct'])),
                    'votes': votes
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['office'].split(',')[0].strip().upper() not in self.target_offices

class NVCountyLoader(NVBaseLoader):
    """
    Loads Nevada county-level results for 2000-2014 elections.
    """

    def load(self):
        # use first row as headers, not pre-canned list
        # need to use OCD_ID from jurisdiction in mapping
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                jurisdiction = self.mapping['name']
                if row['party'] and row['party'] != '&nbsp;':
                    rr_kwargs['party'] = row['party'].strip()
                rr_kwargs.update({
                    'jurisdiction': jurisdiction,
                    'ocd_id': self.mapping['ocd_id'],
                    'votes': int(row['votes'].replace(',','').strip())
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        if self.mapping['election'] == 'nv-2004-09-07-primary' or self.mapping['election'] == 'nv-2002-09-03-primary':
            skip = row['office'].split(' (')[0].split(' DISTRICT')[0] not in self.target_offices
        else:
            skip = row['office'].split(',')[0].strip().upper() not in self.target_offices
        return skip

class NVXmlLoader(NVBaseLoader):
    """
    Parses and loads jurisdiction-level results from Nevada for 2012 and 2014.
    """

    jurisdiction_attrs = set([
        'Carson City',
        'Churchill',
        'Clark',
        'Douglas',
        'Elko',
        'Esmeralda',
        'Eureka',
        'Humboldt',
        'Lander',
        'Lincoln',
        'Lyon',
        'Mineral',
        'Nye',
        'Pershing',
        'Storey',
        'Washoe',
        'White Pine'
    ])

    def load(self):
        # need to pluck OCD_ID from jurisdictions
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as xmlfile:
            tree = etree.parse(xmlfile)
            races = tree.xpath('/USNVResults/UIPage/Race')
            for race in races:
                if self._skip_row(race.attrib['RaceTitle']):
                    continue
                rr_kwargs = self._common_kwargs.copy()
                rr_kwargs.update(self._build_contest_kwargs(race))
                for candidate in race.xpath('Candidate'):
                    rr_kwargs.update(self._build_candidate_kwargs(candidate))
                    for jurisdiction in self.jurisdiction_attrs:
                        result_kwargs = (self._build_jurisdiction_kwargs(candidate, jurisdiction))
                        if not result_kwargs['votes'] == 'N/A':
                            rr_kwargs.update(result_kwargs)
                            results.append(RawResult(**rr_kwargs))
            RawResult.objects.insert(results)

    def _skip_row(self, race_title):
        return race_title.split(',')[0].upper() not in self.target_offices

    def _build_candidate_kwargs(self, candidate):
        return {
            'full_name': candidate.attrib['CandidateName'].strip(),
            'party': candidate.attrib['Party'],
            'incumbent': candidate.attrib['IsIncumbent'],
            'winner': candidate.attrib['IsWinner']
        }

    def _build_jurisdiction_kwargs(self, candidate, jurisdiction):
        j_obj = [j for j in self.datasource._jurisdictions() if j['jurisdiction'] == jurisdiction][0]
        key = jurisdiction.replace(' ','')+'Votes'
        return { 'jurisdiction': j_obj['jurisdiction'], 'ocd_id': j_obj['ocd_id'], 'votes': candidate.attrib[key]}

    def _build_contest_kwargs(self, race):
        office = race.attrib['RaceTitle'].split(', ')[0].strip()
        try:
            district = race.attrib['District']
        except KeyError:
            if 'District' in race.attrib['RaceTitle']:
                district = race.attrib['RaceTitle'].split(', ')[1].split('District ')[1].split()[0]
            else:
                district = None
        return {
            'office': office,
            'district': district,
            'total_precincts': race.attrib['TotalPrecincts'],
            'precincts_reported': race.attrib['PrecinctsReported']
        }
