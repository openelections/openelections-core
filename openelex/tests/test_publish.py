from unittest import TestCase

from openelex.base.publish import ResultFileFinder 

class TestResultFileFinder(TestCase):
    def test_build_glob(self):
        # Tuples of arguments/expected values
        # Tuples are packed as:
        # state, datefilter, raw, ext, expected
        search_dir = "openelex/us/bakery"
        test_values = [
            ('md', '2000', True, ".csv", "openelex/us/bakery/2000*__md__*__raw.csv"),
            ('md', None, True, ".csv", "openelex/us/bakery/*__md__*__raw.csv"),
        ]

        for state, datefilter, raw, ext, expected in test_values:
            glob_str = ResultFileFinder.build_glob(state, search_dir, ext,
                datefilter=datefilter, raw=raw)
            self.assertEqual(glob_str, expected)
