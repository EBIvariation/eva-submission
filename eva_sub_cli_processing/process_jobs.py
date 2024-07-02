from datetime import datetime

import requests
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry


def _url_build(*args, **kwargs):
    url = cfg.query('submissions', 'webservice', 'url') + '/' + '/'.join(args)
    if kwargs:
        return url + '?' + '&'.join(f'{k}={v}' for k, v in kwargs.items())
    else:
        return url


@retry(tries=5, backoff=2, jitter=.5)
def _get_submission_api(url):
    auth = (cfg.query('submissions', 'webservice', 'admin_username'), cfg.query('submissions', 'webservice', 'admin_password'))
    response = requests.get(url, auth=auth)
    response.raise_for_status()
    return response.json()


@retry(tries=5, backoff=2, jitter=.5)
def _put_submission_api(url):
    auth = (cfg.query('submissions', 'webservice', 'admin_username'), cfg.query('submissions', 'webservice', 'admin_password'))
    response = requests.put(url, auth=auth)
    response.raise_for_status()
    return response.json()


def process_submissions():

    scanner = NewSubmissionScanner()
    ready_submissions = scanner.scan()

    if not ready_submissions:
        return 0
    else:
        # Process a defined set of new dataset found. Run through Cron, this will result in one new pipeline
        # being kicked off per interval.
        for ready_submission in ready_submissions:
            return _process_submission(ready_submission)


def _process_submission(submission):
    # TODO: Create submission processing directory
    submission.start()


class SubmissionScanner(AppLogger):

    statuses = []

    def scan(self):
        submissions = []
        for status in self.statuses:
            for submission_data in _get_submission_api(_url_build('admin', 'submissions', 'status', status)):
                submissions.append(Submission(
                    submission_id=submission_data.get('submissionId'),
                    submission_status=submission_data.get('status'),
                    uploaded_time=submission_data.get('uploadedTime')
                ))
        return submissions

    def report(self):
        header = ['Submission Id', 'Submission status', 'Uploaded time']
        scan = self.scan()
        lines = []
        for submission in scan:
            lines.append((submission.submission_id, submission.submission_status, str(submission.uploaded_time)))
        pretty_print(header, lines)


class NewSubmissionScanner(SubmissionScanner):

    statuses = ['UPLOADED']


class Submission(AppLogger):

    def __init__(self, submission_id, submission_status, uploaded_time):
        self.submission_id = submission_id
        self.submission_status = submission_status
        self.uploaded_time = uploaded_time

    def start(self):
        response = _put_submission_api(_url_build('admin', 'submission', self.submission_id,  'status', 'PROCESSING'))
        self.submit_pipeline()

    def submit_pipeline(self):
        # TODO: Actually submit a job for this submission
        pass

    def __repr__(self):
        return f'Submission(submission_id={self.submission_id}, submission_status={self.submission_status}, ' \
               f'uploaded_time={self.uploaded_time})'
