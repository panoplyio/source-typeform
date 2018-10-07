import copy
import panoply
import requests
import backoff
import datetime


BATCH_SIZE = 1000
DESTINATION = 'typeform'
DESTINATION_POSTFIX = '{__table}'
BASE_URL = 'https://api.typeform.com'
FORM_RESPONSES_URL = BASE_URL + '/forms/{value}/responses'
DATE_PARSER_FORMAT = '%Y-%m-%dT%H:%M:%S'


def _log_backoff(details):
    """ Log each time a backoff happened """

    print ('Backing off {wait:0.1f} seconds afters {tries} tries '
           'calling function {target} with args {args} and kwargs '
           '{kwargs}'.format(**details))


class Typeform(panoply.DataSource):

    # constructor
    def __init__(self, source, options):
        super(Typeform, self).__init__(source, options)

        source['destination'] = source.get('destination') or DESTINATION
        # append the destination postfix
        if DESTINATION_POSTFIX not in source['destination']:
            source['destination'] += '_{}'.format(DESTINATION_POSTFIX)

        self._incval = get_incval(source)

        forms = source.get('forms', [])
        self._forms = copy.deepcopy(forms)

        # add an 'after' attribute used
        # for pagination for each different form
        for form in self._forms:
            form['after'] = None

        self._access_token = source.get('access_token')
        self._total = len(self._forms)

    def read(self, n=None):
        if not self._forms:
            # no more data to consume
            return None

        form = self._forms[0]

        # construct the GET url and make the request
        params = self._build_params(form, n)
        url = FORM_RESPONSES_URL.format(**form)
        response = self._request(url, params)

        items = response.get('items', [])
        if not items:
            # we're done paginating with the current form.
            # no more results for this form, remove it
            self._forms.pop(0)
        else:
            # prepare the offset to the next set of records
            form['after'] = items[-1].get('token')

        # report progress
        loaded = self._total - len(self._forms)
        msg = '%s of %s forms fetched' % (loaded, self._total)
        self.progress(loaded, self._total, msg)

        results = prepare_results(form, response)
        return results

    def get_forms(self):
        """ GET all the user's forms """
        self.log('Get forms')
        url = BASE_URL + '/forms'

        # the body of the result is a list of forms
        response = self._request(url)
        forms = response.get('items', [])
        return map(lambda f: dict(name=f.get('title'), value=f.get('id')), forms)

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=5,
                          on_backoff=_log_backoff)
    def _request(self, url, params=None):
        """ Helper function for issuing GET requests """
        self.log('Send Typefrom request', url, params)
        headers = {
            'authorization': 'Bearer {}'.format(self._access_token)
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        self.log('Received Typefrom response', response.url)
        return response.json()

    def _build_params(self, form, batch_size):
        """ construct the relevant params according to the Typeform API """
        page_size = batch_size if batch_size else BATCH_SIZE
        params = {
            'page_size': page_size,
        }

        # pull data incrementally if configured to do so.
        if self._incval:
            params['since'] = self._incval

        if form['after']:
            params['after'] = form['after']

        return params


def prepare_results(form, results):
    """ Add metadata and flatten the results """
    items = results.get('items', [])
    for item in items:
        item_id = item['token']
        _answers = []
        answers = item.get('answers', [])

        for answer in answers:
            new_answer = {}
            for key, value in answer.iteritems():
                '''
                Flatten the answers data, for example:
                
                'field': {
                    'id': 'some_id',
                    'type': "some_type"
                }
                
                turns to:
                
                'field_id': 'some_id',
                'field_type': "some_type"
                '''
                if isinstance(value, dict):
                    for k, v in value.iteritems():
                        new_key = '{}_{}'.format(key, k)
                        new_answer[new_key] = v
                        if k == 'id':
                            id_val = '{}-{}'.format(item_id, v)
                            new_answer['id'] = id_val
                            new_answer['__parent_id'] = item_id

                else:
                    new_answer[key] = value
            _answers.append(new_answer)

        add_item_data(form, item, _answers)
    return items


def add_item_data(form, item, answers):
    """ Add the flatten data and metadata to each item """
    # 'completed' represent the number of completed forms that
    # returned from the server
    item['__completed'] = True
    if not answers:
        item['__completed'] = False
    item['answers'] = answers
    item['id'] = item['token']
    item['__table'] = form['name']


def get_incval(source):
    """ create incval using lastTimeSucceed if exists """
    if not source.get('lastTimeSucceed'):
        return None

    time = source.get('lastTimeSucceed').split(".")[0]
    # convert the str to a datetime
    time = datetime.datetime.strptime(time, DATE_PARSER_FORMAT)
    # reduce 13 hours from the lastTimeSucceed
    # to support all the different time zones
    new_time = time - datetime.timedelta(hours=13)
    # convert the datetime back to str and return it
    return datetime.datetime.strftime(new_time, DATE_PARSER_FORMAT)
