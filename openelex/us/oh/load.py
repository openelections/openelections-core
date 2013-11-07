from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import unicodecsv

"""
load() accepts an object from filenames.json

Usage:

from openelex.us.oh import load
l = load.LoadResults()
file = l.filenames[0] # just an example
l.load(file)
"""

class LoadResults(BaseLoader):
    
    def load(self, file):
        connect('openelex_oh_test')
        print file['generated_name']
        with open(join(self.cache_dir, file['generated_name']), 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            if 'precinct' in file['generated_name']:
                reporting_level = 'precinct'
                jurisdiction = file['name']
            else:
                reporting_level = 'county'
                jurisdiction = file['name']
            for row in reader:
                pass