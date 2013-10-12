from os.path import exists, join
from openelex.base.load import BaseLoader
from openelex.us.md import fetch
from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import unicodecsv
import requests
import datetime

"""
load() accepts an object from filenames.json

Usage:

from openelex.us.md import load
l = load.LoadResults()
file = l.filenames['2002'][0] # just an example
l.load(file)
"""

class LoadResults(BaseLoader):
    
    def load(self, file):
        connect('openelex_md_test')
        print file['generated_name']
        contest = self.get_contest(file)
        if contest.year == 2002:
            self.load_2002_file(file, contest)
        else:
            self.load_non2002_file(file, contest)
        contest.updated = datetime.datetime.now()
        contest.save()
    
    def elections(self, year):
        url = "http://openelections.net/api/v1/state/%s/year/%s/" % (self.state, year)
        response = json.loads(requests.get(url).text)
        return response['elections']
    
    def get_contest(self, file):
        year = int(file['generated_name'][0:4])
        election = [e for e in self.elections(year) if e['id'] == file['election']][0]
        start_year, start_month, start_day = election['start_date'].split('-')
        end_year, end_month, end_day = election['end_date'].split('-')
        contest, created = Contest.objects.get_or_create(state=self.state, year=year, election_id=election['id'], start_date=datetime.date(int(start_year), int(start_month), int(start_day)), end_date=datetime.date(int(end_year), int(end_month), int(end_day)), election_type=election['election_type'], result_type=election['result_type'], special=election['special'])
        if created == True:
            contest.created = datetime.datetime.now()
            contest.save()
        return contest
    
    def load_2002_file(self, file, contest):
        headers = ['office', 'fill', 'jurisdiction', 'last', 'middle', 'first', 'party', 'winner', 'vote_type', 'votes', 'fill2']
        with open(join(self.cache_dir, file['generated_name']), 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, delimiter='|', encoding='latin-1')
            reporting_level = 'county'
            jurisdictions = self.jurisdiction_mappings(('ocd','fips','urlname','name'))
            for row in reader:
                if row['jurisdiction'].strip() == 'Baltimore City':
                    jurisdiction = [x for x in jurisdictions if x['name'] == "Baltimore City"][0]
                else:
                    jurisdiction = [x for x in jurisdictions if x['name']+" County" == row['jurisdiction'].strip()][0]
                if row['winner'] == '1':
                    winner = True
                else:
                    winner = False
                if row['last'].strip() == 'zz998':
                    name = row['last'].strip()
                    candidate = Candidate(state=self.state.upper(), family_name=name)
                    write_in=True
                else:
                    cand_name = self.combine_name_parts([row['first'], row['middle'], row['last']])
                    name = self.parse_name(cand_name)
                    write_in=False
                    candidate = Candidate(state=self.state.upper(), given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                contest.update(push__results=(self.load_county_2002(row, jurisdiction['ocd'], jurisdiction['name'], candidate, reporting_level, write_in, winner)))
    
        
    
    def load_non2002_file(self, file, contest):
        with open(join(self.cache_dir, file['generated_name']), 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            if 'state_legislative' in file['generated_name']:
                reporting_level = 'state_legislative'
                districts = [j for j in reader.fieldnames if j not in ['Vote Type','Vote Type (FOR=1,AGAINST=2)', 'County', 'Candidate Name', 'Party', 'Office Name', 'Office District', 'Winner', 'Write-In?']]
            elif 'precinct' in file['generated_name']:
                reporting_level = 'precinct'
                jurisdiction = file['name']
            else:
                reporting_level = 'county'
                jurisdiction = file['name']
            for row in reader:
                if not row['Office Name'].strip() in ['President - Vice Pres', 'U.S. Senator', 'U.S. Congress', 'Governor / Lt. Governor', 'Comptroller', 'Attorney General', 'State Senator', 'House of Delegates']:
                    continue
                # parse candidate - we get full names
                name = self.parse_name(row['Candidate Name'])
                # if office is president, then skip state in lookup, otherwise use
                if row['Office Name'] == 'President - Vice Pres':
                    candidate = Candidate(state='US', given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                else:
                    candidate = Candidate(state=self.state.upper(), given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                # sometimes write-in field not present
                try:
                    if row['Write-In?'] == 'Y':
                        write_in = True
                    else:
                        write_in = False
                except KeyError:
                    write_in = None
                if row['Winner'] == 'Y':
                    winner = True
                else:
                    winner = False
                if reporting_level == 'state_legislative':
                    contest.update(push_all__results=(self.load_state_legislative(row, districts, candidate, reporting_level, write_in, winner)))
                elif reporting_level == 'county':
                    contest.update(push__results=(self.load_county(row, file['ocd_id'], jurisdiction, candidate, reporting_level, write_in, winner)))
                elif reporting_level == 'precinct':
                    contest.update(push__results=(self.load_precinct(row, file['ocd_id'], jurisdiction, candidate, reporting_level, write_in, winner)))
        
    
    def load_state_legislative(self, row, districts, candidate, reporting_level, write_in, winner):
        results = []
        for district in districts:
            if row[district].strip() == '':
                total_votes = 0
            else:
                total_votes = int(float(row[district]))
            if row['Party'] not in candidate.raw_parties:
                candidate.raw_parties.append(row['Party'])
            ocd = "ocd-division/country:us/state:md/sldl:%s" % district.strip().split(' ')[1]
            results.append(Result(ocd_id=ocd, jurisdiction=district.strip(), raw_office=row['Office Name'].strip()+' '+row['Office District'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes, winner=winner))
        return results
    
    def load_county_2002(self, row, ocd_id, jurisdiction, candidate, reporting_level, write_in, winner):
        total_votes = int(float(row['votes']))
        return Result(ocd_id=ocd_id, jurisdiction=jurisdiction, raw_office=row['office'].strip(), reporting_level=reporting_level, candidate=candidate, party=row['party'].strip(), write_in=write_in, total_votes=total_votes, vote_breakdowns={})
    
    def load_county(self, row, ocd_id, jurisdiction, candidate, reporting_level, write_in, winner):
        try:
            total_votes = int(float(row['Total Votes']))
        except:
            print row
        vote_breakdowns = { 'election_night_total': int(float(row['Election Night Votes'])), 'absentee_total': int(float(row['Absentees Votes'])), 'provisional_total': int(float(row['Provisional Votes'])), 'second_absentee_total': int(float(row['2nd Absentees Votes']))}
        return Result(ocd_id=ocd_id, jurisdiction=jurisdiction, raw_office=row['Office Name']+' '+row['Office District'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes, vote_breakdowns=vote_breakdowns)
        
    def load_precinct(self, row, ocd_id, jurisdiction, candidate, reporting_level, write_in, winner):
        jurisdiction = jurisdiction+' '+str(row['Election District'])+"-"+str(row['Election Precinct'])
        total_votes = int(float(row['Election Night Votes']))
        vote_breakdowns = { 'election_night_total': int(float(row['Election Night Votes']))}
        return Result(ocd_id=ocd_id+"/precinct:"+str(row['Election District'])+"-"+str(row['Election Precinct']), jurisdiction=jurisdiction, raw_office=row['Office Name']+' '+row['Office District'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes, vote_breakdowns=vote_breakdowns)
