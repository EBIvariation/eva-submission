import os.path
from unittest import TestCase
from unittest.mock import patch, Mock

from eva_sub_cli_processing.process_jobs import NewSubmissionScanner
from eva_submission.submission_config import load_config


def patch_get(json_data):
    return patch('requests.get', return_value=Mock(json=Mock(return_value=json_data)))


class TestSubmissionScanner(TestCase):

    resource = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')

    def test_scan(self):
        config_file = os.path.join(self.resource, 'submission_config.yml')
        load_config(config_file)
        scanner = NewSubmissionScanner()
        json_data = [
            {'submissionId': 'sub123', 'status': 'UPLOADED', 'uploadedTime': '2024-05-12'}
        ]

        with patch_get(json_data) as m_get:
            submissions = scanner.scan()
            assert submissions[0].submission_id == 'sub123'
            m_get.assert_called_once_with('https://test.com/admin/submissions/status/UPLOADED', auth=('admin', 'password'))

    def test_report(self):
        config_file = os.path.join(self.resource, 'submission_config.yml')
        load_config(config_file)
        scanner = NewSubmissionScanner()
        json_data = [
            {'submissionId': 'sub123', 'status': 'UPLOADED', 'uploadedTime': '2024-05-12'}
        ]
        with patch_get(json_data) as m_get, patch('builtins.print') as m_print:
            scanner.report()
            m_get.assert_called_once_with('https://test.com/admin/submissions/status/UPLOADED', auth=('admin', 'password'))
        m_print.assert_any_call('| Submission Id | Submission status | Processing step |    Processing status | Last updated time | Priority |')
        m_print.assert_any_call('|        sub123 |          UPLOADED |      VALIDATION | READY_FOR_PROCESSING |        2024-05-12 |        5 |')

