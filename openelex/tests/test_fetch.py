from unittest import TestCase

from openelex.base.fetch import ErrorHandlingURLopener, HTTPError

class TestErrorHandlingURLopener(TestCase):
    def setUp(self):
        self.opener = ErrorHandlingURLopener()

    def test_404(self):
        url = "http://example.com/test.csv"
        self.assertRaises(HTTPError, self.opener.retrieve, url)
