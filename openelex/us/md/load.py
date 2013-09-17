from os.path import exists, join
from openelex.base.load import BaseLoader
from openelex.us.md import fetch
from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import csv

"""
Usage:

from openelex.us.md import load
l = load.LoadResults()
l.run(2012)
"""


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
        connect('openelex_md')
        for f in files:
            with open(join(self.cache_dir, f['generated_name'])) as csvfile:
                reader = csv.DictReader(csvfile)
                if 'state_legislative' in f['generated_name']:
                    reporting_level = 'state_legislative'
                    districts = [j for j in reader.fieldnames if j not in ['Vote Type (FOR=1,AGAINST=2)', 'County', 'Candidate Name', 'Party', 'Office Name', 'Office District', 'Winner', 'Write-In?']]
                elif 'county' in f['generated_name']:
                    reporting_level = 'county'
                elif 'precinct' in f['generated_name']:
                    reporting_level = 'precinct'
                else:
                    reporting_level = 'state'
                    jurisdiction = "ocd-division/country:us/state:md"
                for row in reader:
                    if not row['Office Name'].strip() in ['President - Vice Pres', 'U.S. Senator', 'U.S. Congress', 'Governor / Lt. Governor', 'Comptroller', 'Attorney General', 'State Senator', 'House of Delegates']:
                        pass
                    # parse candidate - we get full names
                    ##### TODO: UTF8 everywhere!
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
                    if row['Winner'] == 'Y':
                        winner = True
                    else:
                        winner = False
                    if reporting_level == 'state_legislative':
                        for district in districts:
                            print district
                            if row[district].strip() == '':
                                votes = 0
                            else:
                                votes = row[district]
                            if row['Party'] not in candidate.raw_parties:
                                candidate.raw_parties.append(row['Party'])
                            ocd = "ocd-division/country:us/state:md/sldl:%s" % district.strip().split(' ')[1]
                            result = Result(ocd_id=ocd, jurisdiction=district.strip(), raw_office=row['Office Name'].strip(), reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=votes, winner=winner).save()
                    elif reporting_level == 'county':
                        jurisdiction = f['name']
                        total_votes = row['Total Votes']
                        vote_breakdowns = { 'election_night_total': row['Election Night Votes'], 'absentee_total': row['Absentees Votes'], 'provisional_total': row['Provisional Votes'], 'second_absentee_total': row['2nd Absentees Votes']}
                    elif reporting_level == 'precinct':
                        jurisdiction = str(row['Election District'])+"-"+str(row['Election Precinct'])
                        total_votes = row['Election Night Votes']
                        vote_breakdowns = { 'election_night_total': row['Election Night Votes']}
                    if not reporting_level == 'state_legislative':
                        result = Result(ocd_id=f['ocd_id'], jurisdiction=jurisdiction, raw_office=row['Office Name'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes, vote_breakdowns=vote_breakdowns).save()
        