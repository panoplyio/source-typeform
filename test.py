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

    def setUp(self):
        self.original_urlopen = urllib2.urlopen

    # clean up mocks after each test
    def tearDown(self):
        urllib2.urlopen = self.original_urlopen

    def test_destination(self):
        source = {'key': 'TypeformAPIKey'}
        Typeform(source, OPTIONS)
        self.assertEqual(source['destination'],
            DESTINATION + DESTINATION_POSTFIX
        )

    def test_iterate_forms(self):
        source = {
            'key': 'TypefromAPIKey',
            'forms': [
                {'id': 'abc', 'name': 'Test Survey'},
                {'id': 'edf', 'name': 'Test Survey'}
            ]
        }

        res1, res2 = generateFormResults(1), generateFormResults(1)
        urllib2.urlopen = MagicMock(side_effect=[res1, res2])

        stream = Typeform(source, OPTIONS)
        stream.read()
        stream.read()
        self.assertEqual(urllib2.urlopen.call_count, 2)

        # we're done, it should return None
        self.assertTrue(stream.read() is None)

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

def generateFormResults(size):
    results = [{'id': x} for x in range(0,size)]
    return BytesIO(json.dumps({
        'stats': {
            'responses': {
                'showing': size
            }
        },
        'questions': results,
        'responses': results
    }))


if __name__ == '__main__':
    unittest.main()
