import os.path
import re
import urlparse

import unicodecsv

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib.text import slugify, election_slug, ocd_type_id


class Datasource(BaseDatasource):
    def mappings(self, year=None):
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], item['raw_url']) 
                for item in self.mappings(year)]

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    def _build_metadata(self, year, elections):
        # BOOKMARK

        # TODO: Properly set name and ocd_id when dealing with a wide variety of
        # reporting levels

        # TODO: Figure out the best way to handle zip files.  Right now, I
        # feel like each of the extracted files should have a mapping, but
        # I'll need to figure out how to handle the duplicate raw_urls
        meta_entries = []
        for election in elections:
            meta_entries.extend(self._build_election_metadata(election))
        return meta_entries

    def _build_election_metadata(self, election):
        """
        Return a list of metadata entries for a single election.
        """
        slug = election['slug']

        if slug == 'ar-2000-11-07-general':
            return self._build_election_metadata_2000_general(election)
        elif slug in ('ar-2000-11-07-special-general',
                'ar-2001-09-25-special-primary',
                'ar-2001-10-16-special-primary-runoff',
                'ar-2001-11-20-special-general'):
            return self._build_election_metadata_zipped_special(election)
        else:
            return [{
                    "generated_filename": self._standardized_filename(election), 
                    "raw_url": election['direct_links'][0], 
                    "ocd_id": 'ocd-division/country:us/state:ar',
                    "name": 'Arkansas',
                    "election": election['slug']
            }]

    def _build_election_metadata_2000_general(self, election):
        meta_entries = [] 
        for county in self._counties():
            county_name = county['name']
            filename = self._standardized_filename(election,
                jurisdiction=county_name, reporting_level='precinct',
                extension='.txt')
            raw_extracted_filename = self._raw_extracted_filename_2000_general(county_name)
            meta_entries.append({
                'generated_filename': filename, 
                'raw_url': election['direct_links'][0],
                'raw_extracted_filename': raw_extracted_filename,
                'ocd_id': county['ocd_id'],
                'name': county_name,
                'election': election['slug'],
            })
        return meta_entries

    def _build_election_metadata_zipped_special(self, election):
        meta_entries = []
        url_paths = self._url_paths_for_election(election['slug'])
        for path in url_paths:
            filename_kwargs = {
                'reporting_level': path['reporting_level'],
                'extension': '.txt',
                'office': path['office'],
                'office_district': path['district'],
            }
            if path['reporting_level'] == 'precinct':
                filename_kwargs['jurisdiction'] = path['jurisdiction']
                jurisdiction = path['jurisdiction']
                ocd_id = 'ocd-division/country:us/state:ar/county:{}'.format(ocd_type_id(jurisdiction))
            else:
                jurisdiction = 'Arkansas'
                ocd_id = 'ocd-division/country:us/state:ar'
            filename = self._standardized_filename(election, **filename_kwargs)
            meta_entries.append({
                'generated_filename': filename,
                'raw_url': path['url'],
                'raw_extracted_filename': path['raw_extracted_filename'],
                'ocd_id': ocd_id,
                'name': jurisdiction, 
                'election': election['slug'],
            })
        return meta_entries

    def _standardized_filename(self, election, **kwargs):
        reporting_level = kwargs.get('reporting_level', None)
        jurisdiction = kwargs.get('jurisdiction', None)
        office = kwargs.get('office', None)
        office_district = kwargs.get('office_district', None)
        extension = kwargs.get('extension',
            self._filename_extension(election))

        bits = [
            election['start_date'].replace('-', ''),
            self.state,
        ]

        if election['special']:
            bits.append('special')

        bits.append(election['race_type'].replace('-', '_'))

        if jurisdiction:
            bits.append(slugify(jurisdiction))

        if office:
            bits.append(slugify(office))

        if office_district:
            bits.append(slugify(office_district))

        if reporting_level:
            bits.append(reporting_level)

        return "__".join(bits) + extension 

    def _raw_extracted_filename_2000_general(self, county_name):
        county_part = county_name + " County"
        county_part = county_part.upper().replace(' ', '') 
        return "cty{}.txt".format(county_part[:7])

    def _filename_extension(self, election):
        parts = urlparse.urlparse(election['direct_links'][0])
        root, ext = os.path.splitext(parts.path)
        return ext

    def _counties(self):
        county_ocd_re = re.compile(r'ocd-division/country:us/state:ar/county:[^/]+$')
        return [m for m in self.jurisdiction_mappings()
                if county_ocd_re.match(m['ocd_id'])]

    def _url_paths(self):
        # TODO: Make this align with other state modules, perhaps move to
        # BaseDatasource
        try:
            return self._cached_url_paths
        except AttributeError:
            filename = os.path.join(PROJECT_ROOT, self.mappings_dir, 'url_paths.csv')
            self._cached_url_paths = []
            with open(filename, 'rU') as csvfile:
                reader = unicodecsv.DictReader(csvfile)
                for row in reader:
                    self._cached_url_paths.append(self._parse_url_path(row))
            return self._cached_url_paths

    def _parse_url_path(self, row):
        clean_row = row.copy()
        clean_row['special'] = row['special'].lower() == 'true'
        clean_row['election_slug'] = election_slug('ar', clean_row['date'],
            clean_row['race_type'], clean_row['special']) 
        return clean_row

    def _url_paths_for_election(self, slug):
        return [p for p in self._url_paths() if p['election_slug'] == slug]
