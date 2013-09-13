from os.path import exists, join
from openelex.base.load import BaseLoader
from openelex.us.md import fetch
import json
import csv

class LoadResults(BaseLoader):
    
    def run(self, year):
        files = self.filenames[year]
        check_cache(files)
        prepare_files(files)
        
    def check_cache(self, year, files):
        uncached_files = []
        for f in files:
            if not exists(join(self.cache_dir, f['generated_name'])):
                uncached_files.append(f)
        if len(uncached_files) > 0:
            fetch_files(year, uncached_files)
        
    def fetch_files(self, year, files):
        f = fetch.FetchResults()
        f.run(year, files)
        # logging?
    
    def process_files(self, files):
        for f in files:
            with open(join(self.cache_dir, f['generated_name'])) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # lookup party from party mappings?
                    # lookup office from office mappings?
                    # if office is president, then skip state in lookup, otherwise use
                    office, created = Office.objects.get_or_create()
                    # parse candidate
                    name = parse_name(row['Candidate Name'])
                    # if office is president, then skip state in lookup, otherwise use
                    candidate, created = Candidate.objects.get_or_create(state=self.state.upper(), given_name=name.first, family_name=name.last, name=name.full_name)
                    # build result