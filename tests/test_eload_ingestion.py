import glob
import os
import shutil
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

    def tearDown(self) -> None:
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_3'))
        for eload in eloads:
            shutil.rmtree(eload)

    # TODO write these tests
    def test_get_db_name(self):
        pass

    def test_check_variant_db(self):
        pass

    def test_check_variant_db_missing(self):
        pass

    def test_load_from_ena(self):
        # TODO can we pass in a testing config rather than loading from file?
        # (would require either tearDown cleaning up everything, or no more write on delete for config...)
        self.eload = EloadIngestion(4)
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_no_project_accession(self):
        self.eload = EloadIngestion(3)
        with self.assertRaises(ValueError):
            self.eload.load_from_ena()

    def test_load_from_ena_script_fails(self):
        self.eload = EloadIngestion(4)
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()
