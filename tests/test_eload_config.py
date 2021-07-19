import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.submission_config import EloadConfig


def touch(f):
    open(f, 'w').close()


class TestEloadConfig(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        self.eload_cfg = EloadConfig()
        self.eload_cfg.load_config_file(os.path.join(self.resources_folder, 'testconfig.yml'))

    def tearDown(self) -> None:
        self.eload_cfg.clear()
        for i in range(5):
            if os.path.exists(self.eload_cfg.config_file + '.' + str(i)):
                os.remove(self.eload_cfg.config_file + '.' + str(i))

    def test_add_to_config(self):
        self.eload_cfg.set('key', value='Value1')
        assert self.eload_cfg.content['key'] == 'Value1'

        self.eload_cfg.set('level1', 'level2', 'level3', value='Value2')
        assert self.eload_cfg.content['level1']['level2']['level3'] == 'Value2'

    def test_remove_from_config(self):
        self.eload_cfg.set('level1', 'level2', 'level3', value='value')
        assert self.eload_cfg.pop('level1', 'lunch time', default='spaghetti') == 'spaghetti'
        assert self.eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'value'
        assert self.eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'spaghetti'

    def test_backup(self):
        touch(self.eload_cfg.config_file)
        for i in range(4):
            touch(self.eload_cfg.config_file + '.' + str(i))
        self.eload_cfg.backup()
        assert not os.path.exists(self.eload_cfg.config_file)
        for i in range(5):
            assert os.path.exists(self.eload_cfg.config_file + '.' + str(i))
