"""
Wyoming offers XLS files containing precinct-level results for each county for all years except 2006.
Elections in 2012 and 2010 are contained in zip files on the SoS site and These are represented in the dashboard API
as the `direct_links` attribute on elections. Zip files for 2000, 2002, 2004 and 2008 elections were sent by the SoS 
and are stored in the https://github.com/openelections/openelections-data-wy repository in the `raw` directory. 2000
files have been converted from the original Quattro Pro source to XLS files.

For 2006, precinct-level results are contained in county-specific PDF files. The CSV versions of those are contained in the 
https://github.com/openelections/openelections-data-wy repository.
"""
from os.path import join
import json
import datetime
import urlparse

from openelex import PROJECT_ROOT
from openelex.base.datasource import BaseDatasource
from openelex.lib import build_github_url

class Datasource(BaseDatasource):
    
    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts containing source url and 
        standardized filename for raw results file, along 
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        "Get list of source data urls, optionally filtered by year"
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], self._url_for_fetch(item)) 
                for item in self.mappings(year)]

    def unprocessed_filename_url_pairs(self, year=None):
        return [(item['generated_filename'].replace(".csv", ".pdf"), item['raw_url'])
                for item in self.mappings(year)
                if item['pre_processed_url']]

    # PRIVATE METHODS

    def _build_metadata(self, year, elections):
        meta = []
        year_int = int(year)
        if year != 2006:
            for election in elections:
                results = [x for x in self._url_paths() if x['date'] == election['start_date']]
                for result in results:
                    print result['county']
                    county = [c for c in self._jurisdictions() if c['county'] == result['county']][0]
                    generated_filename = self._generate_county_filename(result, election)
                    meta.append({
                        "generated_filename": generated_filename,
                        'raw_url': result['url'],
                        'raw_extracted_filename': result['raw_extracted_filename'],
                        "ocd_id": county['ocd_id'],
                        "name": county['county'],
                        "election": election['slug']
                    })
        else:
            for election in elections:
                csv_links = self._find_csv_links(election['direct_links'][0])
                counties = self._jurisdictions()
                results = zip(counties, csv_links[1:])
                for result in results:
                    meta.append({
                        "generated_filename": self._generate_county_filename(result[0]['county'], election),
                        "pre_processed_url": None,
                        "raw_url": result[1],
                        "ocd_id": result[0]['ocd_id'],
                        "name": result[0]['county'],
                        "election": election['slug']
                    })
        return meta
        
    def _generate_county_filename(self, result, election):
        if election['race_type'] == 'general':
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                election['race_type'],
                result['county'].lower().replace(' ','_')
            ]
        elif result['party'] == '':
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                election['race_type'],
                result['county'].lower().replace(' ','_')
            ]
        else:
            bits = [
                election['start_date'].replace('-',''),
                self.state.lower(),
                result['party'],
                election['race_type'],
                result['county'].lower().replace(' ','_')
            ]
        return "__".join(bits) + '.csv'

    def _jurisdictions(self):
        """Wyoming counties"""
        m = self.jurisdiction_mappings()
        mappings = [x for x in m if x['county'] != ""]
        return mappings

    def _url_for_fetch(self, mapping):
        if mapping['pre_processed_url']:
            return mapping['pre_processed_url']
        else:
            return mapping['raw_url']
