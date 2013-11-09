from os.path import dirname, exists, join
from os import listdir
import inspect
import json

from nameparser import HumanName
import csv
import unicodecsv

from .state import StateBase

class BaseLoader(StateBase):
    """
    Base class for loading results data into MongoDB
    Intended to be subclassed in state-specific load.py modules.
    Reads from cached resources inside each state directory.
    """
    
    
    def __init__(self):
        super(BaseLoader, self).__init__()
        #TODO: use datasource.mappings instead
        #self.filenames = json.loads(open(join(self.mappings_dir,'filenames.json'), 'r').read())
        #TODO: use mappings instead
        self.cached_files = listdir(self.cache_dir)

    def run(self):
        raise NotImplementedError()

    #TODO: Migrate name parsing bits to a transforms/name module or function
    def parse_name(self, name):
        return HumanName(name)
        
    def combine_name_parts(self, bits):
        # expects a list of name bits in order
        return " ".join([x.strip() for x in bits])
    
    def jurisdiction_mappings(self, headers):
        "Given a tuple of headers, returns a JSON object of jurisdictional mappings based on OCD ids"
        filename = join(self.mappings_dir, self.state+'.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)
