import os
from datetime import datetime
from unittest import TestCase

from openelex.exceptions import UnsupportedFormatError

try:
    from openelex.tasks.bake import state_file
except ImportError:
    state_file = None

from openelex.base.bake import Baker

class TestBakeStateFileTask(TestCase):
    def test_task_exists(self):
        self.assertIsNot(state_file, None, "Task bake.state_file does not exist.")

class TestBaker(TestCase):
    def test_filename(self):
        baker = Baker('md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        fmt = 'json'
        filename = baker.filename(fmt=fmt, timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615.json')

    def test_manifest_filename(self):
        baker = Baker('md')
        ts = datetime(2014, 2, 11, 10, 56, 15)
        filename = baker.manifest_filename(timestamp=ts)
        self.assertEqual(filename, 'md_20140211T105615_manifest.txt')

    def test_write_unsupported_format(self):
        baker = Baker('md')
        self.assertRaises(UnsupportedFormatError, baker.write, 'xml')

    def test_default_outputdir(self):
        baker = Baker('md')
        path = os.path.join(os.path.join('openelex', 'us', 'bakery'))
        outputdir = baker.default_outputdir()
        self.assertTrue(outputdir.endswith(path))
