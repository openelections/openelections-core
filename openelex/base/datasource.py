from os.path import dirname, exists, join
from urllib import urlretrieve
import inspect
import json
import os
import urlparse

import requests
import unicodecsv

from openelex import PROJECT_ROOT
from .state import StateBase

class BaseDatasource(StateBase):
    """
    Wrapper for interacting with source data.

    Primary use serving as an interface to a state's source data, such as URLs
    of raw data files, and for standardizing names of result files.

    Intended to be subclassed in state-specific datasource.py modules.

    """
    def elections(self):
        raise NotImplementedError()

    def mappings(self):
        raise NotImplementedError()

    def target_urls(self):
        raise NotImplementedError()

    def filename_url_pairs(self):
        "Returns array of tuples of standardized filename, source url pairs"
        raise NotImplementedError()

    def standardized_filename(self, url, fname):
        """A standardized, fully qualified path name"""
        #TODO:apply filename standardization logic
        # non-result pages/files use default urllib name conventions
        # result files need standardization logic (TBD)
        if fname:
            filename = join(self.cache_dir, fname)
        else:
            filename = self.filename_from_url(url)
        return filename

    def filename_from_url(self, url):
        #TODO: this is quick and dirty
        # see urlretrieve code for more robust conversion of
        # url to local filepath
        result = urlparse.urlsplit(url)
        bits = [
            self.cache_dir,
            result.netloc + '_' +
            result.path.strip('/'),
        ]
        name = join(*bits)
        return name

    def clear_filenames(self):
        open(join(PROJECT_ROOT, self.mappings_dir, 'filenames.json'), 'w').close() 

    def jurisdiction_mappings(self):
        "Returns a JSON object of jurisdictional mappings based on OCD ids"
        filename = join(PROJECT_ROOT, self.mappings_dir, self.state + '.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)

    def filename_mappings(self):
        filename = join(PROJECT_ROOT, self.mappings_dir, 'filenames.json')
        with open(filename) as f:
            try:
                mappings = json.loads(f.read())
            except:
                mappings = {}
            return mappings

    def update_mappings(self, year, filenames):
        mappings = self.filename_mappings()
        try:
            del mappings[str(year)]
        except:
            pass
        mappings[str(year)] = filenames
        with open(join(PROJECT_ROOT, self.mappings_dir, 'filenames.json'), 'w') as f:
            json.dump(mappings, f, indent=2)
