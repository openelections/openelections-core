from os.path import dirname, exists, join
from urllib import urlretrieve
import inspect
import json
import os
import urlparse

import requests
import unicodecsv

class DataSource(object):
    """
    Base class for interacting with source data.
    Primary use is fetching data files from source and standardizing names of files,
    which are then cached on S3 by their standardized name and used downstream to load
    results into data store.

    Intended to be subclassed in state-specific stdz/datasource.py modules.

    """

    def __init__(self, source_id):
        self.source_id = source_id
        self.state = self.__module__.split('.')[-2]
        # Save files to cache/ dir inside state directory
        self.cache_dir = join(dirname(inspect.getfile(self.__class__)), 'cache')
        self.mappings_dir = join(dirname(inspect.getfile(self.__class__)), 'mappings')
        try:
            os.makedirs(self.cache_dir)
        except OSError:
            pass
        try:
            os.makedirs(self.mappings_dir)
        except OSError:
            pass
        # Check for the ocd mappings csv and the filenames.json files;
        # if they don't exist, create them.
        open(join(self.mappings_dir, self.state+'.csv'), 'a').close()
        open(join(self.mappings_dir, 'filenames.json'), 'a').close()

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

    def clear_filenames(self):
        open(join(self.mappings_dir, 'filenames.json'), 'w').close() 

    def jurisdiction_mappings(self, headers):
        "Given a tuple of headers, returns a JSON object of jurisdictional mappings based on OCD ids"
        filename = join(self.mappings_dir, self.state+'.csv')
        with open(filename, 'rU') as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers)
            mappings = json.dumps([row for row in reader])
        return json.loads(mappings)

    def filename_mappings(self):
        filename = join(self.mappings_dir, 'filenames.json')
        with open(filename) as f:
            try:
                mappings = json.loads(f.read())
            except:
                mappings = {}
            return mappings

    def update_mappings(self, year, filenames):
        mappings = self.filename_mappings()
        try:
            mappings[str(year)].update(filenames)
        except KeyError:
            mappings[str(year)] = filenames
        with open(join(self.mappings_dir, 'filenames.json'), 'w') as f:
            json.dump(mappings, f)

    def api_response(self, state, year):
        url = "http://openelections.net/api/v1/state/%s/year/%s/" % (state, year)
        response = json.loads(requests.get(url).text)
        return response
