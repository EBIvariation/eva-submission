import os.path
from unittest import TestCase
from unittest.mock import patch, Mock

from eva_sub_cli_processing.process_jobs import NewSubmissionScanner
from eva_submission.submission_config import load_config


def patch_get_multiple(json_data_list):
    return patch('requests.get', return_value=Mock(json=Mock(side_effect=json_data_list)))


class TestSubmissionScanner(TestCase):

    resource = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')

    def test_scan(self):
        config_file = os.path.join(self.resource, 'submission_config.yml')
        load_config(config_file)
        scanner = NewSubmissionScanner()
        json_data_list = [
            #  The first call to status/UPLOADED
            [{'submissionId': 'sub123', 'status': 'UPLOADED', 'uploadedTime': '2024-05-12'}],
            # The second call to VALIDATION/FAILURE
            [{'submissionId': 'sub124', 'step': 'VALIDATION', 'status': 'FAILURE', 'priority':5, 'lastUpdateTime': '2024-05-12'}]
        ]

        with patch_get_multiple(json_data_list) as m_get:
            submissions = scanner.scan()
            assert submissions[0].submission_id == 'sub123'
        m_get.assert_any_call('https://test.com/admin/submissions/status/UPLOADED', auth=('admin', 'password'))
        m_get.assert_any_call('https://test.com/admin/submission-processes/VALIDATION/FAILURE', auth=('admin', 'password'))
        assert m_get.call_count == 2


    def test_report(self):
        config_file = os.path.join(self.resource, 'submission_config.yml')
        load_config(config_file)
        scanner = NewSubmissionScanner()
        json_data_list = [
            #  The first call to status/UPLOADED
            [{'submissionId': 'sub123', 'status': 'UPLOADED', 'uploadedTime': '2024-05-12'}],
            # The second call to VALIDATION/FAILURE
            [{'submissionId': 'sub124', 'step': 'VALIDATION', 'status': 'FAILURE', 'priority':5, 'lastUpdateTime': '2024-05-12'}]
        ]

        with patch_get_multiple(json_data_list) as m_get, patch('builtins.print') as m_print:
            scanner.report()
        m_get.assert_any_call('https://test.com/admin/submissions/status/UPLOADED', auth=('admin', 'password'))
        m_get.assert_any_call('https://test.com/admin/submission-processes/VALIDATION/FAILURE', auth=('admin', 'password'))
        assert m_get.call_count == 2
        m_print.assert_any_call('| Submission Id | Submission status | Processing step |    Processing status | Last updated time | Priority |')
        m_print.assert_any_call('|        sub123 |          UPLOADED |      VALIDATION | READY_FOR_PROCESSING |        2024-05-12 |        5 |')
        m_print.assert_any_call('|        sub124 |        PROCESSING |      VALIDATION |              FAILURE |        2024-05-12 |        5 |')
        assert m_print.call_count == 3


