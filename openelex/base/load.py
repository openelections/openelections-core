from os.path import join
import json

import unicodecsv

from .state import StateBase


class BaseLoader(StateBase):
    """
    Base class for loading results data into MongoDB
    Intended to be subclassed in state-specific load.py modules.
    Reads from cached resources inside each state directory.
    
    Subclasses should create RawResult models and only do minimal
    cleaning such as:

    * Strip leading/trailing whitespace from values
    * Convert votes from string to integer

    All other cleaning or transformation of values should be
    done by creating transforms.

    """

    def __init__(self):
        super(BaseLoader, self).__init__()
        #TODO: use datasource.mappings instead
        #self.filenames = json.loads(open(join(self.mappings_dir,'filenames.json'), 'r').read())

    def run(self, mapping):
        raise NotImplementedError()

    def jurisdiction_mappings(self, headers):
        "Given a tuple of headers, returns a JSON object of jurisdictional mappings based on OCD ids"
        filename = join(self.mappings_dir, self.state+'.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)
