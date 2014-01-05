import inspect
import logging
import os
import re
import sys

import boto

from .state import StateBase

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from openelex import settings
from .cache import StateCache


class BaseArchiver(StateBase):
    """
    Interface to S3 for storing/retrieving result files on S3.
    """

    def __init__(self, state, bucket='openelex-data'):
        super(BaseArchiver, self).__init__(state)
        self.s3_path = "us/states/%s/raw/" % self.state
        self.local_cache = StateCache(self.state)
        self.conn = S3Connection(settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY)
        self.bucket = self.conn.get_bucket(bucket)

    def save_file(self, path):
        """Saves file in state cache to S3

        Path should be absolute. Returns S3 Key instance

        """
        ky = Key(self.bucket)
        key = os.path.join(self.s3_path, path.rsplit('/')[-1])
        ky.key = key
        ky.set_contents_from_filename(path)
        return ky

    def get_file(self, key):
        """Retrieve file from S3 and save to state's local cache."""
        key_obj = self.bucket.get_key(key)
        local_path = os.path.join(self.local_cache.path, key)
        key_obj.get_contents_to_filename(local_path)
        return key_obj

    def delete_file(self, key):
        """Delete file from S3. Returns deleted key."""
        key_obj = self.bucket.get_key(key)
        key_obj.delete()
        return key_obj

    def keys(self, datefilter=''):
        """List S3 keys for state, optionally limited by datefilter.

        Returns array of S3 key instances.

        """
        date_clean = datefilter.replace('-','')
        target_dates = []
        keys = []
        if datefilter:
            for key in self.bucket.list(self.s3_path):
                if re.search(r'%s' % date_clean, key.key):
                    keys.append(key)
        else:
            keys = list(self.bucket.list(self.s3_path))
        return keys

    #TODO
    def save_manifest(self):
        pass
