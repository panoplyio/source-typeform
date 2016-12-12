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
        self.original_fetch_limit = typeform.FETCH_LIMIT

    # clean up mocks after each test
    def tearDown(self):
        urllib2.urlopen = self.original_urlopen
        typeform.FETCH_LIMIT = self.original_fetch_limit

    def test_destination(self):
        source = {'key': 'TypeformAPIKey'}
        Typeform(source, OPTIONS)
        self.assertEqual(source['destination'],
            DESTINATION + DESTINATION_POSTFIX
        )

    def test_results(self):
        form_name = 'Test Survey'
        source = {
            'key': 'TypefromAPIKey',
            'forms': [{'value': 'abc', 'name': form_name}]
        }

        # mock the returned responses from the server
        responses = [{'id': 1, 'field_id': 'x', 'token': 'y', 'foo': 'bar'}]
        form_result = generateFormResults(1, responses)
        urllib2.urlopen = MagicMock(return_value=form_result)

        stream = Typeform(source, OPTIONS)

        results = stream.read()

        # each of the records should hold the appropriate destination
        # name, while the first record is always the general statistics
        # and from then on it's questions and responses records
        self.assertEqual(results[1].get('foo'), 'bar')
        self.assertEqual(results[2].get('foo'), 'bar')

        self.assertEqual(results[0].get('__table'), '_stats')
        self.assertEqual(results[1].get('__table'), '_questions')
        self.assertEqual(results[2].get('__table'), '_responses')

        # it should add the formid_idsuffix
        self.assertEqual(results[1].get('id'), 'abc_x')
        self.assertEqual(results[2].get('id'), 'abc_y')

        # it should keep the original id, if exists
        self.assertEqual(results[1].get('oid'), 1)

        # all the records should belong to the same form
        for record in results:
            self.assertEqual(record.get('__form'), form_name)

    def test_incremental(self):
        source = {
            'key': 'TypeformAPIKey',
            'lastTimeSucceed': '2016-09-21T10:23:42.819Z',
            'forms': [{'value': 'abc', 'name': 'Test Survey'}]
        }

        res = generateFormResults(1)
        urllib2.urlopen = MagicMock(return_value=res)

        stream = Typeform(source, OPTIONS)
        stream.read()

        time = source.get('lastTimeSucceed')
        time = typeform.getTimestamp(time)
        # the focus here is on the 'since' query param that indicates
        # we're pulling data after a specific date
        url = '%s/form/%s?key=%s&completed=true&offset=0&limit=%s&since=%s' % (
            BASE_URL,
            source['forms'][0].get('value'),
            source['key'],
            FETCH_LIMIT,
            time
        )
        urllib2.urlopen.assert_called_with(url)

    def test_pagination(self):
        source = {
            'key': 'TypeformAPIKey',
            'forms': [{'value': 'abc', 'name': 'Test Survey'}]
        }

        limit = typeform.FETCH_LIMIT = 1
        res1, res2 = generateFormResults(1, None, 2), generateFormResults(1)
        urllib2.urlopen = MagicMock(side_effect=[res1, res2])

        stream = Typeform(source, OPTIONS)
        stream.read() # 1st page
        stream.read() # 2nd page
        self.assertIsNone(stream.read()) # we're done

        # it should make 2 requests.
        self.assertEqual(urllib2.urlopen.call_count, 2)

        # test that it constructed the correct url
        url = '%s/form/%s?key=%s&completed=true&offset=1&limit=%s' % (
            BASE_URL,
            source['forms'][0].get('value'),
            source['key'],
            limit
        )
        urllib2.urlopen.assert_called_with(url)

    def test_iterate_forms(self):
        source = {
            'key': 'TypefromAPIKey',
            'forms': [
                {'value': 'abc', 'name': 'Test Survey'},
                {'value': 'edf', 'name': 'Test Survey'}
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
            'forms': [{'value': 'someid', 'name': 'Test Survey'}]
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
            'forms': [{'value': 'someid', 'name': 'Test Survey'}]
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

def generateFormResults(size, responses = None, total = None):
    indexes = range(0, size)

    # generate objects if no responses are given.
    if not responses:
        responses = [{'id': x} for x in indexes]

    results = [responses[x] for x in indexes]
    return BytesIO(json.dumps({
        'stats': {
            'responses': {
                'completed': total or size
            }
        },
        'questions': results,
        'responses': results
    }))


if __name__ == '__main__':
    unittest.main()
