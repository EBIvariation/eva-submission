import os
from copy import deepcopy
from unittest import TestCase

from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_migration import EloadMigration
from eva_submission.submission_config import load_config


class TestEloadMigration(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload = EloadMigration(66)
        self.original_config = deepcopy(self.eload.eload_cfg.content)

    def tearDown(self):
        self.eload.eload_cfg.content = self.original_config
        if os.path.isfile(self.eload.eload_cfg.config_file):
            os.remove(self.eload.eload_cfg.config_file)

    def test_update_and_reload_config(self):
        self.eload.update_and_reload_config()
        with open(self.eload.config_path) as config_file:
            contents = config_file.read()
            self.assertEqual(contents.find(cfg['noah']['eloads_dir']), -1)
            self.assertEqual(contents.find(cfg['noah']['projects_dir']), -1)
            self.assertEqual(contents.find(cfg['noah']['genomes_dir']), -1)
