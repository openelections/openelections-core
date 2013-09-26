import logging
import os
import inspect
import sys
import boto

class BaseArchiver(object):
    """
    Base scraper to be subclassed by scrapers for specific states.
    Archive files to S3 if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    are provided.
    """

    def __init__(self):
        self.state = self.__module__.split('.')[-2]
        self.AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.mappings_dir = os.path.join(os.path.dirname(inspect.getfile(self.__class__)), 'mappings')
        self.cache_dir = os.path.join(os.path.dirname(inspect.getfile(self.__class__)), 'cache')
        self.s3_path = "us/states/%s/raw/" % self.state
        self.conn = boto.connect_s3()

    def run(self):
        raise NotImplementedError()
