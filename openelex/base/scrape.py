import logging
import os
import sys

import boto

class BaseScraper(object):
    """
    Base scraper to be subclassed by scrapers for specific states.
    Archive files to S3 if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    are provided.
    """

    def __init__(self, AWS_ACCESS_KEY_ID='', AWS_SECRET_ACCESS_KEY='')
        if not self.AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY:
            print "AWS credentials not provided; will not archive to S3"

        else:
            self.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
            self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

    def run(self):
        raise NotImplementedError()
