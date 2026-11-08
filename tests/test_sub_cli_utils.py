from unittest import TestCase
from unittest.mock import patch, Mock

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing import sub_cli_utils
from eva_sub_cli_processing.sub_cli_utils import get_tracking_details, update_tracking_details


class TestTrackingDetails(TestCase):

    def setUp(self):
        cfg.content = {'submissions': {'webservice': {'url': 'https://sub.ws', 'admin_username': 'admin',
                                                      'admin_password': 'pass'}}}

    def tearDown(self):
        cfg.content = {}

    def test_get_tracking_details(self):
        details = {'projectAccession': 'PRJEB1', 'analyses': [{'analysisAlias': 'a1'}]}
        response = Mock(text='{}', json=Mock(return_value=details))
        with patch.object(sub_cli_utils.requests, 'get', return_value=response) as m_get:
            assert get_tracking_details('sub1') == details
        assert m_get.call_args[0][0] == 'https://sub.ws/admin/submission/sub1/trackingDetails'

    def test_get_tracking_details_failure_returns_none(self):
        with patch.object(sub_cli_utils.requests, 'get', side_effect=ConnectionError('down')):
            assert get_tracking_details('sub1') is None

    def test_get_tracking_details_http_error_returns_none(self):
        response = Mock(text='')
        response.raise_for_status.side_effect = Exception('404')
        with patch.object(sub_cli_utils.requests, 'get', return_value=response):
            assert get_tracking_details('sub1') is None

    def test_update_tracking_details_sends_only_provided_fields(self):
        analyses = [{'analysisAlias': 'a1', 'aggregationType': 'basic'}]
        with patch.object(sub_cli_utils, 'put_to_sub_ws') as m_put:
            update_tracking_details('sub1', project_accession='PRJEB1', project_title='title',
                                    project_taxonomies='9606', analyses=analyses, metadata_json_file='/m.json')
        m_put.assert_called_once_with(
            'https://sub.ws/admin/submission/sub1/trackingDetails',
            {'projectAccession': 'PRJEB1', 'projectTitle': 'title', 'projectTaxonomies': '9606',
             'analyses': analyses, 'metadataJsonFile': '/m.json'}
        )

    def test_update_tracking_details_nothing_to_send(self):
        with patch.object(sub_cli_utils, 'put_to_sub_ws') as m_put:
            update_tracking_details('sub1')
        m_put.assert_not_called()

    def test_update_tracking_details_swallows_errors(self):
        with patch.object(sub_cli_utils, 'put_to_sub_ws', side_effect=Exception('boom')):
            update_tracking_details('sub1', project_accession='PRJEB1')  # must not raise