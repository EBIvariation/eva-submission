import os
import shutil
from unittest import TestCase

from ebi_eva_common_pyutils.config import cfg

from eva_submission.submission_config import load_config
from eva_submission.vep_utils import recursive_nlst, get_vep_and_vep_cache_version_from_ensembl


class TestVepUtils(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

        # Set up vep cache directory and vep
        os.makedirs(cfg['vep_cache_path'], exist_ok=True)
        os.makedirs(os.path.join(cfg['vep_path'], 'ensembl-vep-release-104/vep'), exist_ok=True)

    def tearDown(self):
        shutil.rmtree(cfg['vep_cache_path'])
        shutil.rmtree(cfg['vep_path'])

    def test_get_vep_versions_from_db(self):
        pass

    def test_get_vep_versions_from_ensembl(self):
        # TODO extremely flaky - could mock but also means ingestion will fail a lot...
        vep_version, cache_version = get_vep_and_vep_cache_version_from_ensembl('GCA_000827895.1')
        self.assertEqual(vep_version, 104)
        self.assertEqual(cache_version, 51)
        # TODO test cache downloaded to right place
