import os.path
import re
import urlparse

from bs4 import BeautifulSoup
import requests
import unicodecsv

from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url
from openelex.lib.text import slugify, ocd_type_id


class Datasource(BaseDatasource):
    RESULTS_PORTAL_URL = "http://www.sos.arkansas.gov/electionresults/index.php"
    CLARITY_PORTAL_URL = "http://results.enr.clarityelections.com/AR/"

    # There aren't precinct-level results for these, just a CSV file with
    # summary data for the county.
    no_precinct_urls = [
        "http://results.enr.clarityelections.com/AR/Columbia/42858/111213/",
        "http://results.enr.clarityelections.com/AR/Ouachita/42896/112694/",
        "http://results.enr.clarityelections.com/AR/Union/42914/112664/",
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

    def _standardized_filename(self, election, bits=None, **kwargs):
        reporting_level = kwargs.get('reporting_level', None)
        jurisdiction = kwargs.get('jurisdiction', None)
        office = kwargs.get('office', None)
        office_district = kwargs.get('office_district', None)
        extension = kwargs.get('extension',
            self._filename_extension(election))

        if bits is None:
            bits = []

        bits.extend([
            election['start_date'].replace('-', ''),
            self.state,
        ])

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

    def _build_election_metadata_clarity(self, election, fmt="xml"):
        """
        Return metadata entries for election results provided by the Clarity
        system.

        These results seem to be for elections starting in 2012.

        Keyword Arguments:

        * fmt - Format of results file.  Can be "xls", "txt" or "xml".
                Default is "xml".
        """
        return self._build_election_metadata_clarity_county(election, fmt) +\
            self._build_election_metadata_clarity_precinct(election, fmt)
        
    def _build_election_metadata_clarity_county(self, election, fmt):
        base_url = self._clarity_election_base_url(election['direct_links'][0])

        return [{
            "generated_filename": self._standardized_filename(election,
                reporting_level='county', extension='.'+fmt),
            "raw_extracted_filename": "detail.{}".format(fmt),
            "raw_url": self._clarity_results_url(base_url, fmt), 
            "ocd_id": 'ocd-division/country:us/state:ar',
            "name": 'Arkansas',
            "election": election['slug']
        }]

    def _build_election_metadata_clarity_precinct(self, election, fmt):
        meta_entries = []
        for path in self._clarity_precinct_url_paths(election, fmt):
            jurisdiction = path['jurisdiction']
            ocd_id = 'ocd-division/country:us/state:ar/county:{}'.format(ocd_type_id(jurisdiction))
            filename = self._standardized_filename(election,
                jurisdiction=jurisdiction, reporting_level='precinct', 
                extension='.'+fmt)
            meta_entries.append({
                "generated_filename": filename,
                "raw_extracted_filename": "detail.{}".format(fmt),
                "raw_url": path['url'],
                "ocd_id": ocd_id, 
                "name": jurisdiction, 
                "election": election['slug'],
            })
        return meta_entries
            
    def _clarity_election_base_url(self, url):
        if "/en/" in url:
            return url.split('/en/')[0] + '/'

        parsed = urlparse.urlsplit(url)
        newpath = '/'.join(parsed.path.split('/')[:-1]) + '/'
        parts = (parsed.scheme, parsed.netloc, newpath, parsed.query,
                 parsed.fragment)
        return urlparse.urlunsplit(parts)

    def _clarity_results_url(self, base_url, fmt):
        return "{}reports/detail{}.zip".format(base_url, fmt)

    def _clarity_precinct_url_paths_filename(self, election):
        filename = self._standardized_filename(election, ['url_paths'],
            reporting_level='precinct', extension='.csv')
        return os.path.join(self.mappings_dir, filename)

    def _clarity_precinct_url_paths(self, election, fmt):
        url_paths_filename = self._clarity_precinct_url_paths_filename(election)
        if os.path.exists(url_paths_filename):
            return self._url_paths(url_paths_filename)

        url_paths = []
        for url, county in self._clarity_county_urls(election):
            base_url = self._clarity_election_base_url(url)
            if base_url not in self.no_precinct_urls: 
                url_paths.append({
                    'date': election['start_date'],
                    'office': '',
                    'race_type': election['race_type'],
                    'party': '',
                    'special': election['special'],
                    'url': self._clarity_results_url(base_url, fmt),
                    'reporting_level': 'precinct',
                    'jurisdiction': county,
                })

        with open(url_paths_filename, 'wb') as f:
            fieldnames = ['date', 'office', 'race_type', 'party',
                'special', 'url', 'reporting_level', 'jurisdiction']
            writer = unicodecsv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(url_paths)

        return url_paths

    def _clarity_county_urls(self, election):
        base_url = self._clarity_election_base_url(election['direct_links'][0])
        county_url = base_url + 'en/select-county.html' 
        r = requests.get(county_url)
        r.raise_for_status()
        return [(self._clarity_county_url(path), county) for path, county 
                in self._scrape_county_paths(r.text)]

    def _scrape_county_paths(self, html):
        soup = BeautifulSoup(html)
        return [(o['value'], o.get_text()) for o in soup.select("table li a")]

    def _clarity_county_url(self, path):
        url = self._clarity_election_base_url(self.CLARITY_PORTAL_URL +
            path.lstrip('/'))
        r = requests.get(url)
        r.raise_for_status()
        redirect_path = self._scrape_county_redirect_path(r.text)
        return url + redirect_path

    def _scrape_county_redirect_path(self, html):
        soup = BeautifulSoup(html)
        script_src = soup.find('script')['src']
        match = re.search(r'\./(\d+)/.*', script_src)
        return match.group(1) + '/summary.html'

    def _counties(self):
        county_ocd_re = re.compile(r'ocd-division/country:us/state:ar/county:[^/]+$')
        return [m for m in self.jurisdiction_mappings()
                if county_ocd_re.match(m['ocd_id'])]

    def _url_for_fetch(self, mapping):
        if 'pre_processed_url' in mapping:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
