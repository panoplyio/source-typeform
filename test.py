import json
import unittest
import urllib2

from typeform import *

OPTIONS = {
    # no-op logger during tests
    'logger': lambda *args: None
}

class TestTypeform(unittest.TestCase):
    # before each test
    def setUp(self):
        self.original_urlopen = urllib2.urlopen

    # after each test
    def tearDown(self):
        urllib2.urlopen = self.original_urlopen

    def test_destination(self):
        source = {'key': 'TypeformAPIKey'}
        Typeform(source, OPTIONS)
        self.assertEqual(source['destination'],
            DESTINATION + DESTINATION_POSTFIX
        )

if __name__ == '__main__':
    unittest.main()