from os.path import dirname, exists, join
from urllib import urlretrieve
import inspect
import json
import os
import urlparse

import requests
import unicodecsv

from .state import StateBase

class BaseFetcher(StateBase):
    """
    Base class for interacting with source data.
    Primary use is fetching data files from source and standardizing names of files,
    which are then cached on S3 by their standardized name and used downstream to load
    results into data store.

    Intended to be subclassed in state-specific fetch.py modules.

    """

    def fetch(self, url, fname=None, overwrite=False):
        """Fetch and cache web page or data file

        ARGS

            url - link to download
            fname - file name for local storage in cache directory
            overwrite - if True, overwrite cached copy with fresh donwload

        """
        local_file_name = self._standardized_filename(url, fname)
        if overwrite:
            name, response = urlretrieve(url, local_file_fname)
        else:
            if exists(local_file_name):
                print "File is cached: %s" % local_file_name
            else:
                name, response = urlretrieve(url, local_file_name)
                print "Added to cache: %s" % local_file_name

    def _standardized_filename(self, url, fname):
        """A standardized, fully qualified path name"""
        #TODO:apply filename standardization logic
        # non-result pages/files use default urllib name conventions
        # result files need standardization logic (TBD)
        if fname:
            filename = join(self.cache.abspath, fname)
        else:
            filename = self._filename_from_url(url)
        return filename

    def _filename_from_url(self, url):
        #TODO: this is quick and dirty
        # see urlretrieve code for more robust conversion of
        # url to local filepath
        result = urlparse.urlsplit(url)
        bits = [
            self.cache.abspath,
            result.netloc + '_' +
            result.path.strip('/'),
        ]
        name = join(*bits)
        return name
