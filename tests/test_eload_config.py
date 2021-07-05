import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.submission_config import EloadConfig


class TestEloadConfig(TestCase):
    configs_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'configs')

    def setUp(self):
        self.old_config_name = os.path.join(self.configs_folder, 'old_config.yml')
        self.old_config = EloadConfig(self.old_config_name)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.old_config.content)
        self.updated_config = EloadConfig(os.path.join(self.configs_folder, 'updated_config.yml'))

    def tearDown(self):
        self.old_config.content = self.original_cfg
        if os.path.exists(f'{self.old_config_name}.old'):
            os.remove(f'{self.old_config_name}.old')

    def test_add_to_config(self):
        eload_cfg = EloadConfig()
        eload_cfg.set('key', value='Value1')
        assert eload_cfg.content['key'] == 'Value1'

        eload_cfg.set('level1', 'level2', 'level3', value='Value2')
        assert eload_cfg.content['level1']['level2']['level3'] == 'Value2'

    def test_remove_from_config(self):
        eload_cfg = EloadConfig()
        eload_cfg.set('level1', 'level2', 'level3', value='value')
        assert eload_cfg.pop('level1', 'lunch time', default='spaghetti') == 'spaghetti'
        assert eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'value'
        assert eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'spaghetti'

    def test_upgrade_config(self):
        """Tests config upgrade for a post-brokering config."""
        self.old_config.upgrade_if_needed('analysis alias')
        self.assertEqual(self.updated_config.content, self.old_config.content)

    def test_upgrade_config_no_analysis_alias_passed(self):
        """If no analysis alias is passed, it should retrieve from metadata when possible."""
        with patch('eva_submission.submission_config.EvaXlsxReader') as mock_reader:
            mock_reader.return_value.analysis = [{'Analysis Alias': 'analysis alias'}]
            self.old_config.upgrade_if_needed(analysis_alias=None)
            self.assertEqual(self.updated_config.content, self.old_config.content)

    def test_upgrade_config_already_updated(self):
        """An already up-to-date config shouldn't get modified."""
        original_content = deepcopy(self.updated_config.content)
        self.updated_config.upgrade_if_needed('analysis alias')
        self.assertEqual(original_content, self.updated_config.content)
