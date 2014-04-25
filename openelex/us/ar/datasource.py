import os.path
import urlparse

from openelex.base.datasource import BaseDatasource


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

    def _build_metadata(self, year, elections):
        # BOOKMARK

        # TODO: Properly set name and ocd_id when dealing with a wide variety of
        # reporting levels

        # TODO: Figure out the best way to handle zip files.  Right now, I
        # feel like each of the extracted files should have a mapping, but
        # I'll need to figure out how to handle the duplicate raw_urls
        meta_entries = []
        for election in elections:
            meta_entries.append({
                "generated_filename": self._standardized_filename(election), 
                "raw_url": election['direct_links'][0], 
                "ocd_id": 'ocd-division/country:us/state:ar',
                "name": 'Arkansas',
                "election": election['slug']
            })
        return meta_entries

    def _standardized_filename(self, election):
        bits = [
            election['start_date'].replace('-', ''),
            self.state,
        ]

        if election['special']:
            bits.append('special')

        bits.append(election['race_type'].replace('-', '_'))

        return "__".join(bits) + self._filename_extension(election)

    def _filename_extension(self, election):
        parts = urlparse.urlparse(election['direct_links'][0])
        root, ext = os.path.splitext(parts.path)
        return ext

        
