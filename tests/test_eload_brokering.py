import glob
import os
import shutil
from unittest import TestCase

from eva_submission.eload_brokering import EloadBrokering
from eva_submission.submission_config import load_config
from eva_submission.eload_submission import EloadPreparation


class TestEloadBrokering(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload = EloadBrokering(3)

    def tearDown(self) -> None:
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_3'))
        for eload in eloads:
            shutil.rmtree(eload)

    def test_upload_vcf_files_to_ena_ftp(self):
        vcf_file = ''
        index_file = ''

        self.eload.eload_cfg.set('brokering', 'vcf_files', value={vcf_file: {'index': index_file}})
        self.eload.upload_vcf_files_to_ena_ftp()
