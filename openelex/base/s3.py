import datetime
import os
import re

from boto.s3.key import Key

class S3Connx(object):

    def __init__(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
        from boto.s3.connection import S3Connection
        self.conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.bucket = self.conn.get_bucket('openelex-data')

    def write(self, data_string, filename):
        """ 
        Archive election result file to state directory on s3

        USAGE:
            s3 = S3Connex(ACCESS_ID, SECRET_KEY)
            s3.write(<data_as_string>)

        RETURN

            <boto.s3.key.Key instance>
        """
        # Get file metadata
        kwargs = self.get_file_meta(data_string, snapshot)

        # Instantiate Key
        ky = Key(self.bucket)
        ky.key = kwargs['path']

        # Write the data
        ky.set_contents_from_string(data_string)
        return ky

    def get_file_meta(self, data_string, snapshot=False):
        fields = data_string[:50].split(';')
        test_flag = fields[0]
        election_date = fields[1].replace('-','')
        state = fields[2]

        path = "AP/live/%s/flat/archive/%s/" % (state, election_date)
        file_name =  "%s.txt" % state

        if snapshot:
            timestamp = datetime.datetime.now().isoformat()
            path += "snapshots/"
            # If test data, store snapshots under date-based directory
            # to support storing data for multiple test days
            if test_flag == 't':
                path += "tests/%s/" % timestamp[:10].replace('-','')
            file_name = "%s_%s.txt" % (state, timestamp)

        final_path = os.path.join(path, file_name)

        kwargs = {
            'test_flag':fields[0],
            'election_date':election_date,
            'state':state,
            'resource': file_name,
            'path':final_path,
        }
        return kwargs

    def read(self, state, elec_date):
        """Get data stored on S3.

        USAGE:
            s3 = S3Connx()
            s3.read('NY', '2012-11-06')

        RETURN
            (<boto.s3.key.Key instance>, <data_as_string>)

        """
        elec_date_clean = elec_date.replace('-','')
        kwargs = {'state':state, 'elec_date':elec_date_clean}
        file_path = "us/states/%(state)s/%(elec_date)s.csv" % kwargs
        key = self.bucket.get_key(file_path)
        return key, key.get_contents_as_string()

    def list_data(self, state, year=None):
        """List data stored on S3 for a given state, and optionally, year.

        USAGE:
            >>> s3 = S3Connx()
            >>> s3.list_data('NY')
            >>> s3.list_data('NY', 2012)

        """
        target_dir = 'us/states/%s' %  state
        if year:
            target_dir += '/%s/' % year
        target_dates = []
        for key in self.bucket.list(target_dir):
            try:
                date_match = re.search(r'/(%s\d{4})' % year, key.key).groups()[0]
                target_dates.append(date_match)
            except AttributeError:
                pass
        return target_dates
