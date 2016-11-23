import json
import unittest
import urllib2

from io import BytesIO
from mock import MagicMock
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

    def test_http_error_msg(self):
        source = {
            'key': 'TypefromAPIKey',
            'forms': [{'id': 'someid', 'name': 'Test Survey'}]
        }

        err_msg = 'please provide a valid API key'
        body = BytesIO(json.dumps({
            'message': err_msg,
            'status': 403
        }))

        # mock the HTTPError that should be return from the server
        err = urllib2.HTTPError('url', 403, 'Forbidden', {}, body)
        urllib2.urlopen = MagicMock(side_effect=err)

        stream = Typeform(source, OPTIONS)
        try:
            stream.read()
        except TypeformError, e:
            self.assertEqual(str(e), err_msg)

    def test_http_error_status(self):
        source = {
            'key': 'TypeformAPIKey',
            'forms': [{'id': 'someid', 'name': 'Test Survey'}]
        }

        # mock the HTTPError that should be returned from the server
        code = 403
        body = BytesIO(json.dumps({'status': code}))
        err = urllib2.HTTPError('url', code, 'Forbidden', {}, body)
        urllib2.urlopen = MagicMock(side_effect=err)

        stream = Typeform(source, OPTIONS)
        try:
            stream.read()
        except TypeformError, e:
            self.assertEqual(str(e), 'HTTP StatusCode ' + str(code))


if __name__ == '__main__':
    unittest.main()
