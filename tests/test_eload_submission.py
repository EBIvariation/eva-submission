import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.eload_submission import Eload
from eva_submission.submission_config import EloadConfig, load_config


class TestEload(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.eload = Eload(55)
        self.updated_config = EloadConfig(os.path.join(self.eload.eload_dir, 'updated_config.yml'))
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

    def tearDown(self):
        self.eload.eload_cfg.content = self.original_cfg
        if os.path.exists(f'{self.eload.eload_cfg.config_file}.old'):
            os.remove(f'{self.eload.eload_cfg.config_file}.old')

    def test_upgrade_config(self):
        """Tests config upgrade for a post-brokering config."""
        self.eload.upgrade_config_if_needed('analysis alias')
        self.assertEqual(self.updated_config.content, self.eload.eload_cfg.content)

    def test_upgrade_config_no_analysis_alias_passed(self):
        """If no analysis alias is passed, it should retrieve from metadata when possible."""
        with patch('eva_submission.config_migration.EvaXlsxReader') as mock_reader:
            mock_reader.return_value.analysis = [{'Analysis Alias': 'analysis alias'}]
            self.eload.upgrade_config_if_needed(analysis_alias=None)
            self.assertEqual(self.updated_config.content, self.eload.eload_cfg.content)

    def test_upgrade_config_already_updated(self):
        """An already up-to-date config shouldn't get modified."""
        original_content = deepcopy(self.updated_config.content)
        self.eload.upgrade_config_if_needed('analysis alias')
        self.assertEqual(original_content, self.updated_config.content)
