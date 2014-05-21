from urlparse import urlsplit
import os.path

from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):
    BASE_URL = 'http://sos.iowa.gov/elections'

    def mappings(self, year=None):
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def filename_url_pairs(self, year=None):
        # TODO: Update once preprocessed data is available
        return [(mapping['generated_filename'], mapping['raw_url']) 
                for mapping in self.mappings(year)
                if not mapping['raw_url'].endswith(".pdf")]

    def unprocessed_filename_url_pairs(self, year=None):
        # TODO: Update once preprocessed data is available 
        return [(mapping['generated_filename'], mapping['raw_url'])
                for mapping in self.mappings(year)
                if mapping['raw_url'].endswith(".pdf")]

    def _build_metadata(self, year, elections):
        # TODO: Update once preprocessed data is available 
        meta_entries = []

        for election in elections:
            slug = election['slug']

            from_url_paths = self._url_paths_metadata(election)
            if len(from_url_paths) > 0:
                # In nearly all cases, there are multiple url_paths.csv
                # entries for a given election.
                meta_entries.extend(from_url_paths)
            else:
                # When there aren't construct a metadata entry from
                # the direct links attribute of the election
                meta_entries.extend(self._direct_links_metadata(election))

            if slug == 'ia-2006-11-07-general':
                meta_entries.extend(self._build_metadata_2006_general(election))
            elif slug == 'ia-2010-06-08-primary':
                meta_entries.extend(self._build_metadata_2010_primary(election))
            elif slug == 'ia-2010-11-02-general':
                meta_entries.extend(self._build_metadata_2010_general(election))
            elif slug == 'ia-2012-06-05-primary':
                meta_entries.extend(self._build_metadata_2012_primary(election))
            elif slug == 'ia-2012-11-06-general':
                meta_entries.extend(self._build_metadata_2012_general(election))

        return meta_entries

    def _url_paths_metadata(self, election):
        meta_entries = []
        url_paths = self._url_paths_for_election(election['slug'])

        for path in url_paths:
            winners_file = path['winners'].lower() == "true"
            if winners_file: 
                # For now, skip winner files
                continue

            filename_kwargs = {
                'reporting_level': path['reporting_level'],
                'extension': self._extension_from_url(path['url']),
                'office': path['office'],
                'office_district': path['district'],
            }
            meta_entries.append({
                'generated_filename': self._standardized_filename(election,
                    **filename_kwargs),
                'raw_url': path['url'],
                'ocd_id': 'ocd-division/country:us/state:ia',
                'name': "Iowa", 
                'election': election['slug'],
            })

        return meta_entries

    def _direct_links_metadata(self, election):
        """Get metadata entries for result files listed by the metadata API"""
        meta_entries = []

        if not len(election['direct_links']):
            return meta_entries

        # If we're getting metadata entries from the direct_links field
        # there should only be one link.  Otherwise, we can't
        # distinguish between the normalized filenames since there
        # isn't any other way to determine the reporting level or
        # office of a particular file.
        #
        # If there are more than one direct_links, then entries should
        # be added to the url_paths.csv file.
        assert len(election['direct_links']) == 1

        url = election['direct_links'][0]
        filename_kwargs = {
            'reporting_level': 'county',
            'extension': self._extension_from_url(url),
        }
        meta_entries.append({
            'generated_filename': self._standardized_filename(election,
                **filename_kwargs),
            'raw_url': url,
            'ocd_id': 'ocd-division/country:us/state:ia',
            'name': "Iowa", 
            'election': election['slug'],
        })

        return meta_entries

    def _extension_from_url(self, url):
        """Get the filename extension from a file's full URL"""
        split = urlsplit(url)
        base, ext = os.path.splitext(split.path)
        return ext

    def _precinct_xls_metadata(self, election):
        """
        Get metadata entries for precinct-level results in per-county Excel files.
        """
        meta_entries = []
        year = election['start_date'].split('-')[0]
        base_url = self._precinct_xls_base_url(election)

        if year == '2006':
            name_suffix = "%20County"
        else:
            name_suffix = ""

        for county in self._counties():
            raw_filename = "{}{}.xls".format(county['name'], name_suffix)
            meta_entries.append({
                "generated_filename": self._standardized_filename(election,
                    reporting_level='precinct', jurisdiction=county['name'],
                    extension='.xls'),
                'raw_url': base_url + '/' + raw_filename,
                'ocd_id': county['ocd_id'],
                'name': county['name'], 
                'election': election['slug'],
            })

        return meta_entries

    def _precinct_xls_base_url(self, election):
        year = election['start_date'].split('-')[0]
        bits = [
            self.BASE_URL,
            'results',
            'xls',
        ]

        if int(year) >= 2008:
            bits.append(year)

        if int(year) >= 2010:
            bits.append(election['race_type'])

        return '/'.join(bits)

    def _build_metadata_2006_general(self, election):
        return self._precinct_xls_metadata(election)

    def _build_metadata_2008_general(self, election):
        meta_entries = self._precinct_xls_metadata(election)

        # In addition to the results in Excel format at 
        # http://sos.iowa.gov/elections/results/xls/2008/Lyon.xls
        # There are also precinct-level results in PDF format that include
        # the precinct absentee breakdown at URLs like
        # http://sos.iowa.gov/elections/pdf/2008/adair.pdf
        for county in self._counties():
            raw_filename = "{}.pdf".format(county['name'])
            meta_entries.append({
                "generated_filename": self._standardized_filename(election,
                    reporting_level='precinct', jurisdiction=county['name'],
                    extension='.pdf'),
                'raw_url': self.BASE_URL + '/pdf/'  + raw_filename,
                'ocd_id': county['ocd_id'],
                'name': county['name'], 
                'election': election['slug'],
            })

        return meta_entries

    def _build_metadata_2010_primary(self, election):
        return self._precinct_xls_metadata(election)

    def _build_metadata_2010_general(self, election):
        return self._precinct_xls_metadata(election)

    def _build_metadata_2012_primary(self, election):
        return self._precinct_xls_metadata(election)

    def _build_metadata_2012_general(self, election):
        return self._precinct_xls_metadata(election)
