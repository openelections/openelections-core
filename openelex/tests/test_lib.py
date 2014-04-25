from unittest import TestCase

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
