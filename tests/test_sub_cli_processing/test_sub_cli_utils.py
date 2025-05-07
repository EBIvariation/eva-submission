import json
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_utils import download_metadata_json_file_for_submission_id, \
    get_metadata_json_for_submission_id
from eva_submission import ROOT_DIR


class TestSubCliToEloadConverter(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    sub_cli_test_dir = os.path.join(resources_folder, 'sub_cli_test_dir')
    cfg.load_config_file(os.path.join(resources_folder, 'submission_config.yml'))

    def setUp(self) -> None:
        if os.path.exists(self.sub_cli_test_dir):
            shutil.rmtree(self.sub_cli_test_dir)
        os.mkdir(self.sub_cli_test_dir)

    def tearDown(self) -> None:
        if os.path.exists(self.sub_cli_test_dir):
            shutil.rmtree(self.sub_cli_test_dir)

    @patch("requests.get")
    def test_download_metadata_json_file_for_submission_id(self, mock_requests):
        mock_response = mock_requests.return_value
        mock_response.status_code = 200
        input_json_file = os.path.join(self.resources_folder, 'input_json_for_json_to_xlsx_converter.json')
        with open(input_json_file) as json_file:
            input_json_data = json.load(json_file)
        mock_response.json.return_value = {"metadataJson": input_json_data}

        submission_id = "submission123"
        metadata_json_file_path = os.path.join(self.sub_cli_test_dir, 'metadata_json.json')
        download_metadata_json_file_for_submission_id(submission_id, metadata_json_file_path)

        # Check if requests.get was called with the correct URL
        mock_requests.assert_called_once_with(
            f"{cfg['submissions']['webservice']['url']}/admin/submission/{submission_id}",
            auth=(cfg['submissions']['webservice']['admin_username'],
                  cfg['submissions']['webservice']['admin_password']))

        # Check if file was written correctly
        assert os.path.exists(metadata_json_file_path)
        with open(metadata_json_file_path) as json_file:
            json_data = json.load(json_file)
        self.assertEqual(input_json_data, json_data)

    @patch("requests.get")
    def test_get_metadata_json_for_submission_id(self, mock_requests):
        mock_response = mock_requests.return_value
        mock_response.status_code = 200
        input_json_file = os.path.join(self.resources_folder, 'input_json_for_json_to_xlsx_converter.json')
        with open(input_json_file) as json_file:
            input_json_data = json.load(json_file)
        mock_response.json.return_value = {"metadataJson": input_json_data}

        submission_id = "submission123"
        response_json_data = get_metadata_json_for_submission_id(submission_id)

        # Check if requests.get was called with the correct URL
        mock_requests.assert_called_once_with(
            f"{cfg['submissions']['webservice']['url']}/admin/submission/{submission_id}",
            auth=(cfg['submissions']['webservice']['admin_username'],
                  cfg['submissions']['webservice']['admin_password']))

        self.assertEqual(input_json_data, response_json_data)
