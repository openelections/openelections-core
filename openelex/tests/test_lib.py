from unittest import TestCase

from openelex.lib import format_date, standardized_filename 
from openelex.lib.text import ocd_type_id, election_slug

class TestText(TestCase):
    def test_ocd_type_id(self):
        # Test that function converst spaces to underscores and
        # non-word characters to tildes
        self.assertEqual(ocd_type_id("Prince George's"),
            u"prince_george~s")
        # Test that leading zeros are stripped by default
        self.assertEqual(ocd_type_id("03D"),
            u"3d")
        # Test that we can force keeping leading zeros
        self.assertEqual(ocd_type_id("03D", False),
            u"03d")
        # Test that hyphens are not escaped
        self.assertEqual(ocd_type_id("001-000-1"),
            u"1-000-1")
        # Test that leading zero stripping can be supressed.
        self.assertEqual(ocd_type_id("001-000-1", False),
            u"001-000-1")

    def test_election_slug(self):
        elec_attrs = {
          'md-2012-11-06-general': {
              'state': 'md',
              'start_date': '2012-11-06',
              'special': False,
              'race_type': 'general',
          },
          'md-2008-06-17-special-general': {
              'state': 'md',
              'start_date': '2008-06-17',
              'special': True,
              'race_type': 'general',
          },
          'ar-2011-05-10-special-primary-runoff': {
              'state': 'ar',
              'start_date': '2011-05-10',
              'special': True,
              'race_type': 'primary-runoff',
          },
        }

        for slug, attrs in elec_attrs.items():
            self.assertEqual(election_slug(**attrs), slug)


class TestLib(TestCase):
    def test_standardized_filename(self):
        # Test with bare minimum args
        kwargs = {
            'start_date': "2012-04-03",
            'state': 'md',
            'extension': ".csv",
        }
        expected = "20120403__md.csv"
        filename = standardized_filename(**kwargs)
        self.assertEqual(filename, expected)

        # Test with more complicated example
        kwargs = {
            'start_date': "2012-04-03",
            'state': 'md',
            'party': "Republican",
            'race_type': "primary",
            'jurisdiction': "Prince George's",
            'reporting_level': 'precinct',
            'extension': ".csv",
        }
        expected = "20120403__md__republican__primary__prince_georges__precinct.csv"
        filename = standardized_filename(**kwargs)
        self.assertEqual(filename, expected)

        # Test with suffix bits
        kwargs = {
            'start_date': "2012-04-03",
            'state': 'md',
            'extension': ".csv",
            'suffix_bits': ['raw']
        }
        expected = "20120403__md__raw.csv"
        filename = standardized_filename(**kwargs)
        self.assertEqual(filename, expected)

    def test_format_date(self):
        test_values = [
            ("20101106", "2010-11-06"),
            ("201011", "2010-11"),
            ("2010", "2010"),
        ]
        for input_date, expected in test_values:
            self.assertEqual(format_date(input_date), expected)

        self.assertRaises(ValueError, format_date, "201011-06")
