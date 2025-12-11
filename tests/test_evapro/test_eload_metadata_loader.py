import os
from unittest import TestCase

from eva_submission.evapro.eload_metadata_loader import EloadMetadataJsonLoader
from eva_submission.submission_config import load_config


class TestEloadMetadataJsonLoader(TestCase):
    top_dir =os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    resources_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')


    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload_metadata_json_loader = EloadMetadataJsonLoader(104)


    def test_get_experiment_types(self):
        assert self.eload_metadata_json_loader.get_experiment_types('ERZ2499196') == ['Whole genome sequencing']


