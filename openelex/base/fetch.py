from os.path import dirname, exists, join
import inspect
import os
from urllib import urlretrieve
import urlparse
import requests
import json

class BaseScraper(object):
    """
    Base class for downloading result files.
    Intended to be subclassed in state-specific fetch.py modules.

    Caches resources inside each state directory.

    """

    def __init__(self):
        self.state = self.__module__.split('.')[-2]
        # Save files to cache/ dir inside state directory
        self.cache_dir = join(dirname(inspect.getfile(self.__class__)), 'cache')
        try:
            os.makedirs(self.cache_dir)
        except OSError:
            pass

    def run(self):
        msg = "You must implement the %s.run method" % self.__class__.__name__
        raise NotImplementedError(msg)

    def fetch(self, url, fname=None, overwrite=False):
        """Fetch and cache web page or data file

        ARGS

            url - link to download
            fname - file name for local storage in cache_dir
            overwrite - if True, overwrite cached copy with fresh donwload

        """
        local_file_name = self.standardized_filename(url, fname)
        #import pdb;pdb.set_trace()
        if overwrite:
            name, response = urlretrieve(url, local_file_fname)
        else:
            if exists(local_file_name):
                print "File is cached: %s" % local_file_name
            else:
                name, response = urlretrieve(url, local_file_name)
                print "Added to cache: %s" % local_file_name

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
        
    def api_response(self, state, year):
        url = "http://dashboard.openelections.net/api/state/%s/year/%s/" % (state, year)
        response = json.loads(requests.get(url).text)
        
    
