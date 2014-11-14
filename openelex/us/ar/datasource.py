import os.path
import re
import urlparse

from bs4 import BeautifulSoup
import requests
import clarify
import unicodecsv

from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url
from openelex.lib.text import ocd_type_id


class Datasource(BaseDatasource):
    RESULTS_PORTAL_URL = "http://www.sos.arkansas.gov/electionresults/index.php"
    CLARITY_PORTAL_URL = "http://results.enr.clarityelections.com/AR/"

    # There aren't precinct-level results for these, just a CSV file with
    # summary data for the county.
    no_precinct_urls = [
        "http://results.enr.clarityelections.com/AR/Columbia/42858/111213/en/summary.html",
        "http://results.enr.clarityelections.com/AR/Ouachita/42896/112694/en/summary.html",
        "http://results.enr.clarityelections.com/AR/Union/42914/112664/en/summary.html",
    ]

    def mappings(self, year=None):
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], self._url_for_fetch(item))
                for item in self.mappings(year)]

    def unprocessed_filename_url_pairs(self, year=None):
        return [(item['generated_filename'].replace(".csv", ".pdf"), item['raw_url'])
                for item in self.mappings(year)
                if 'pre_processed_url' in item]

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    def _build_metadata(self, year, elections):
        meta_entries = []
        for election in elections:
            meta_entries.extend(self._build_election_metadata(election))
        return meta_entries

    def _build_election_metadata(self, election):
        """
        Return a list of metadata entries for a single election.
        """
        slug = election['slug']
        link = election['direct_links'][0]

        if slug == 'ar-2000-11-07-general':
            return self._build_election_metadata_2000_general(election)
        elif slug in ('ar-2000-11-07-special-general',
                'ar-2001-09-25-special-primary',
                'ar-2001-10-16-special-primary-runoff',
                'ar-2001-11-20-special-general'):
            return self._build_election_metadata_zipped_special(election)
        elif link.startswith(self.CLARITY_PORTAL_URL):
            return self._build_election_metadata_clarity(election)
        else:
            return self._build_election_metadata_default(election)

    def _build_election_metadata_default(self, election):
        link = election['direct_links'][0]
        filename_kwargs = {}

        if link.startswith(self.RESULTS_PORTAL_URL):
            # Report portal results are precinct-level
            filename_kwargs['reporting_level'] = 'precinct'
            # And the format is tab-delimited text
            filename_kwargs['extension'] = '.tsv'

        generated_filename = self._standardized_filename(election, **filename_kwargs)
        mapping = {
            "generated_filename": generated_filename,
            "raw_url": link,
            "ocd_id": 'ocd-division/country:us/state:ar',
            "name": 'Arkansas',
            "election": election['slug']
        }

        if "2002" in election['slug']:
            generated_filename = generated_filename.replace('.pdf', '.csv')
            mapping['pre_processed_url'] = build_github_url(self.state,
                generated_filename)
            mapping['generated_filename'] = generated_filename

        return [mapping]

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


    def _raw_extracted_filename_2000_general(self, county_name):
        county_part = county_name + " County"
        county_part = county_part.upper().replace(' ', '')
        return "cty{}.txt".format(county_part[:7])


    def _build_election_metadata_clarity(self, election, fmt="xml"):
        """
        Return metadata entries for election results provided by the Clarity
        system.

        These results seem to be for elections starting in 2012.

        Keyword Arguments:

        * fmt - Format of results file.  Can be "xls", "txt" or "xml".
                Default is "xml".
        """
        base_url = election['direct_links'][0]
        jurisdiction = clarity.Jurisdiction(url=base_url, level='state')
        return self._build_election_metadata_clarity_county(election, fmt, jurisdiction) +\
            self._build_election_metadata_clarity_precinct(election, fmt, jurisdiction)

    def _build_election_metadata_clarity_county(self, election, fmt, jurisdiction):

        return [{
            "generated_filename": self._standardized_filename(election,
                reporting_level='county', extension='.'+fmt),
            "raw_extracted_filename": "detail.{}".format(fmt),
            "raw_url": jurisdiction.report_url(fmt),
            "ocd_id": 'ocd-division/country:us/state:ar',
            "name": 'Arkansas',
            "election": election['slug']
        }]

    def _build_election_metadata_clarity_precinct(self, election, fmt, jurisdiction):
        meta_entries = []
        for path in self._clarity_precinct_url_paths(election, fmt, jurisdiction):
            jurisdiction_name = path['jurisdiction']
            ocd_id = 'ocd-division/country:us/state:ar/county:{}'.format(ocd_type_id(jurisdiction_name))
            filename = self._standardized_filename(election,
                jurisdiction=jurisdiction_name, reporting_level='precinct',
                extension='.'+fmt)
            meta_entries.append({
                "generated_filename": filename,
                "raw_extracted_filename": "detail.{}".format(fmt),
                "raw_url": path['url'],
                "ocd_id": ocd_id,
                "name": jurisdiction_name,
                "election": election['slug'],
            })
        return meta_entries

    def _clarity_precinct_url_paths_filename(self, election):
        filename = self._standardized_filename(election, ['url_paths'],
            reporting_level='precinct', extension='.csv')
        return os.path.join(self.mappings_dir, filename)

    def _clarity_precinct_url_paths(self, election, fmt, jurisdiction):
        url_paths_filename = self._clarity_precinct_url_paths_filename(election)
        if os.path.exists(url_paths_filename):
            return self._url_paths(url_paths_filename)

        url_paths = []
        for subjurisdiction in jurisdiction.get_subjurisdictions():
            if subjurisdiction.url not in self.no_precinct_urls:
                url_paths.append({
                    'date': election['start_date'],
                    'office': '',
                    'race_type': election['race_type'],
                    'party': '',
                    'special': election['special'],
                    'url': subjurisdiction.report_url(fmt),
                    'reporting_level': 'precinct',
                    'jurisdiction': subjurisdiction.name,
                })

        with open(url_paths_filename, 'wb') as f:
            fieldnames = ['date', 'office', 'race_type', 'party',
                'special', 'url', 'reporting_level', 'jurisdiction']
            writer = unicodecsv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(url_paths)

        return url_paths

    def _url_for_fetch(self, mapping):
        if 'pre_processed_url' in mapping:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
