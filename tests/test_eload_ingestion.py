import os
import subprocess
from unittest import TestCase
from unittest.mock import patch

from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config


class TestEloadIngestion(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        cfg.content['executable'] = {
            'load_from_ena': 'path_to_load_script'
        }
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

    def test_load_from_ena(self):
        # TODO can we pass in a testing config rather than loading from file?
        self.eload = EloadIngestion(4)
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_no_project_accession(self):
        self.eload = EloadIngestion(2)
        with self.assertRaises(ValueError):
            self.eload.load_from_ena()

    def test_load_from_ena_script_fails(self):
        self.eload = EloadIngestion(4)
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError('Something terrible happened', 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()
