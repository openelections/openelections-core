"""
load() accepts an object from Datasource.mappings

Usage:

    from openelex.us.md import load
    l = load.LoadResults()
    file = l.filenames['2000'][49] # just an example
    l.load(mapping)

"""
from os.path import exists, join
import datetime
import json
import requests
import unicodecsv

from mongoengine import *

from openelex.base.load import BaseLoader
from openelex.models import Candidate, Result, Contest

class LoadResults(BaseLoader):
    
    def run(self, mapping):
        print mapping['generated_filename']
        contest = self.get_contest(mapping)
        if contest.year == 2002:
            self.load_2002_file(mapping, contest)
        elif contest.year == 2000 and contest.election_type == 'primary': # special case for 2000 primary
            self.load_2000_primary_file(mapping, contest)
        else:
            self.load_non2002_file(mapping, contest)
        contest.updated = datetime.datetime.now()
        contest.save()
    
    def elections(self, year):
        url = "http://openelections.net/api/v1/state/%s/year/%s/" % (self.state, year)
        response = json.loads(requests.get(url).text)
        return response['elections']
    
    def get_contest(self, mapping):
        year = int(mapping['generated_filename'][0:4])
        election = [e for e in self.elections(year) if e['id'] == mapping['election']][0]
        start_year, start_month, start_day = election['start_date'].split('-')
        end_year, end_month, end_day = election['end_date'].split('-')
        contest, created = Contest.objects.get_or_create(state=self.state, year=year, election_id=election['id'], start_date=datetime.date(int(start_year), int(start_month), int(start_day)), end_date=datetime.date(int(end_year), int(end_month), int(end_day)), election_type=election['election_type'], result_type=election['result_type'], special=election['special'])
        if created == True:
            contest.created = datetime.datetime.now()
            contest.save()
        return contest
    
    def load_2000_primary_file(self, mapping, contest):
        with open(join(self.cache_dir, mapping['generated_filename']), 'rU') as csvfile:
            lines = csvfile.readlines()
            #TODO: use Datasource.mappings instead of jurisdiction_mappings
            jurisdictions = self.jurisdiction_mappings(('ocd','fips','urlname','name'))
            for line in lines[0:377]:
                if line.strip() == '':
                    continue # skip blank lines
                else: # determine if this is a row with an office
                    cols = line.strip().split(',')
                    if cols[0][1:29] == 'President and Vice President':
                        office_name = 'President - Vice Pres'
                        if 'Democratic' in cols[0]:
                            party = 'DEM'
                        else:
                            party = 'REP'
                        continue
                    elif cols[0][1:13] == 'U.S. Senator':
                        office_name = 'U.S. Senator'
                        if 'Democratic' in cols[0]:
                            party = 'DEM'
                        else:
                            party = 'REP'
                        continue
                    elif cols[0][1:27] == 'Representative in Congress':
                        district = int(cols[0][71:73])
                        office_name = 'U.S. Congress ' + str(district)
                        if 'Democratic' in cols[0]:
                            party = 'DEM'
                        else:
                            party = 'REP'
                        continue
                    # skip offices we don't want
                    elif cols[0][1:21] == 'Judge of the Circuit':
                        continue
                    elif cols[0][1:31] == 'Female Delegates and Alternate':
                        continue
                    elif cols[0][1:29] == 'Male Delegates and Alternate':
                        continue
                    elif cols[0][1:28] == 'Delegates to the Republican':
                        continue
                    elif cols[0] == '""':
                        candidates = [x.replace('"','').strip() for x in cols if x.replace('"','') != '']
                        winner = [i for i, j in enumerate(candidates) if 'Winner' in j][0] # index of winning candidate
                        candidates[winner] = candidates[winner].split(' Winner')[0]
                        # handle name_parse and Uncommitted candidate
                    else: # county results
                        result = [x.replace('"','').strip() for x in cols if x != '']
                        juris = [j for j in jurisdictions if j['name'] == result[0].strip()][0]
                        cand_results = zip(candidates, result[1:])
                        for cand, votes in cand_results:
                            name = self.parse_name(cand)
                            if office_name == 'President - Vice Pres':
                                candidate = Candidate(state='US', given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                            else:
                                candidate = Candidate(state=self.state.upper(), given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)
                            result = Result(ocd_id=juris['ocd'], jurisdiction=juris['name'], raw_office=office_name, reporting_level='county', candidate=candidate, party=party, write_in=False, total_votes=int(votes), vote_breakdowns={})
                            contest.update(push__results=(result))
                                            
                    
        
    
    def load_2002_file(self, mapping, contest):
        headers = ['office', 'fill', 'jurisdiction', 'last', 'middle', 'first', 'party', 'winner', 'vote_type', 'votes', 'fill2']
        with open(join(self.cache_dir, mapping['generated_filename']), 'rU') as csvfile:
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
                result = Result(ocd_id=ocd_id+"/precinct:"+str(row['Election District'])+"-"+str(row['Election Precinct']), jurisdiction=jurisdiction, raw_office=row['Office Name']+' '+row['Office District'], reporting_level=reporting_level, candidate=candidate, party=row['Party'], write_in=write_in, total_votes=total_votes, vote_breakdowns=vote_breakdowns)
                contest.update(push__results=(self.load_county_2002(row, jurisdiction['ocd'], jurisdiction['name'], candidate, reporting_level, write_in, winner)))
    
        
    
    def load_non2002_file(self, mapping, contest):
        with open(join(self.cache_dir, mapping['generated_filename']), 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            if 'state_legislative' in mapping['generated_filename']:
                reporting_level = 'state_legislative'
                districts = [j for j in reader.fieldnames if j not in ['Vote Type','Vote Type (FOR=1,AGAINST=2)', 'County', 'Candidate Name', 'Party', 'Office Name', 'Office District', 'Winner', 'Write-In?']]
            elif 'precinct' in mapping['generated_filename']:
                reporting_level = 'precinct'
                jurisdiction = mapping['name']
            else:
                reporting_level = 'county'
                jurisdiction = mapping['name']
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
                    contest.update(push__results=(self.load_county(row, mapping['ocd_id'], jurisdiction, candidate, reporting_level, write_in, winner)))
                elif reporting_level == 'precinct':
                    contest.update(push__results=(self.load_precinct(row, mapping['ocd_id'], jurisdiction, candidate, reporting_level, write_in, winner)))
        
    
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
