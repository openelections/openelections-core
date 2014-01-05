"""
load() accepts an object from Datasource.mappings

Usage:

    from openelex.us.oh import load
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