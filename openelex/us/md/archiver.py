import os
import json
from openelex.base.archiver import BaseArchiver
import boto
from boto.s3.key import Key

class ResultsArchiver(BaseArchiver):

    def run(self, year):
        filenames = self.filename_mappings()[str(year)]
        bucket = self.conn.get_bucket('openelex-data')
        for file in filenames:
            k = Key(bucket)
            k.key = os.path.join(self.s3_path, file['generated_name'])
            k.set_contents_from_filename(os.path.join(self.cache_dir, file['generated_name']))

    def filename_mappings(self):
        filename = os.path.join(self.mappings_dir, 'filenames.json')
        with open(filename) as f:
            try:
                mappings = json.loads(f.read())
            except:
                mappings = {}
            return mappings
