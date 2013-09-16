from os.path import dirname, exists, join
from os import listdir
import inspect
import json
from nameparser import HumanName

class BaseLoader(object):
    """
    Base class for loading results data into MongoDB
    Intended to be subclassed in state-specific load.py modules.
    Reads from cached resources inside each state directory.
    """
    
    
    def __init__(self):
        self.state = self.__module__.split('.')[-2]
        self.cache_dir = join(dirname(inspect.getfile(self.__class__)), 'cache')
        self.mappings_dir = join(dirname(inspect.getfile(self.__class__)), 'mappings')
        self.filenames = json.loads(open(join(self.mappings_dir,'filenames.json'), 'r').read())
        self.cached_files = listdir(self.cache_dir)

    def run(self):
        msg = "You must implement the %s.run method" % self.__class__.__name__
        raise NotImplementedError(msg)
    
    def parse_name(self, name):
        return HumanName(name)
        
    def combine_name_parts(self, bits):
        # expects a list of name bits in order
        return " ".join(bits)
        
    
        

    
    
            
        
    