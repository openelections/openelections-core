import os
from datetime import datetime
from unittest import TestCase

from mongoengine.context_managers import query_counter

from openelex.exceptions import UnsupportedFormatError

try:
    from openelex.tasks.bake import state_file
except ImportError:
    state_file = None

from openelex.base.bake import Roller, Baker

class TestBakeStateFileTask(TestCase):
    def test_task_exists(self):
        self.assertIsNot(state_file, None, "Task bake.state_file does not exist.")

class TestRoller(TestCase):
    def test_get_list(self):
        roller = Roller()
        data = roller.get_list(state='md', datefilter='20120403',
                type='general', level='state_legislative')
        from pprint import pprint
        pprint(data[0])
        self.fail()

class TestBaker(TestCase):
    def test_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        fmt = 'json'
        filename = baker.filename(fmt=fmt, timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615.json')

    def test_manifest_filename(self):
        baker = Baker(state='md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        filename = baker.manifest_filename(timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615_manifest.txt')

    def test_write_unsupported_format(self):
        baker = Baker(state='md')
        self.assertRaises(UnsupportedFormatError, baker.write, 'xml')

    def test_default_outputdir(self):
        baker = Baker(state='md')
        path = os.path.join(os.path.join('openelex', 'us', 'bakery'))
        outputdir = baker.default_outputdir()
        self.assertTrue(outputdir.endswith(path))

    def test_collect_items(self):
        baker = Baker(state='md', datefilter='20120403',
                type='general', level='state_legislative')
        data = baker.collect_items().get_items()
        print data[0]
            
        self.fail()
