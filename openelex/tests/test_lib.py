from unittest import TestCase

from openelex.lib.text import ocd_type_id

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
