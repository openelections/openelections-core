from os.path import exists, join
from openelex.base.load import BaseLoader
from openelex.us.md import fetch
from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import csv

class LoadResults(BaseLoader):
    
    def run(self, year):
        files = self.filenames[str(year)]
        self.check_cache(str(year), files)
        self.process_files(files)
        
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
        connect('openelex_results')
        for f in files:
            with open(join(self.cache_dir, f['generated_name'])) as csvfile:
                reader = csv.DictReader(csvfile)
                if 'state_legislative' in f['generated_name']:
                    reporting_level = 'state_legislative'
                    # fix ' LEGS 26'
                    districts = [j for j in reader.fieldnames if j not in ['Vote Type (FOR=1,AGAINST=2)', 'County', 'Candidate Name', 'Party', 'Office Name', 'Office District', 'Winner', 'Write-In?']]
                elif 'county' in f['generated_name']:
                    reporting_level = 'county'
                elif 'precinct' in f['generated_name']:
                    reporting_level = 'precinct'
                else:
                    reporting_level = 'state'
                    jurisdiction = "ocd-division/country:us/state:md"
                for row in reader:
                    # parse candidate - we get full names
                    name = self.parse_name(row['Candidate Name'])
                    # if office is president, then skip state in lookup, otherwise use
                    if row['Office Name'] == 'President - Vice Pres':
                        candidate = Candidate(state='US', given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                    else:
                        candidate = Candidate(state=self.state.upper(), given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                    if row['Write-In?'] == 'Y':
                        write_in = True
                    else:
                        write_in = False
                    if reporting_level == 'state_legislative':
                        for district in districts:
                            ocd = "ocd-division/country:us/state:md/sldl:%s" % district.strip().lower()
                            result = Result(ocd_id=ocd, jurisdiction=district.strip(), raw_office=row['Office Name'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=row[district]).save()
                    elif reporting_level == 'county':
                        jurisdiction = f['name']
                        total_votes = row['Total Votes']
                    elif reporting_level == 'precinct':
                        jurisdiction = str(row['Election District'])+"-"+str(row['Election Precinct'])
                        total_votes = row['Election Night Votes']
                    if not reporting_level == 'state_legislative':
                        result = Result(ocd_id=f['ocd_id'], jurisdiction=jurisdiction, raw_office=row['Office Name'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes).save()
        