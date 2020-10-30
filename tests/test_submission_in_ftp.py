import os
from unittest import TestCase

from eva_submission.eload_config import load_config
from eva_submission.submission_in_ftp import FtpDepositBox


class TestFtpDepositBox(TestCase):

    ftp_box = os.path.join(os.path.dirname(__file__), 'resources', 'ftpboxes', 'eva-box-01')

    # def test_find_files_for(self):
    #     vcf_file = os.path.join(self.ftp_box, 'upload', 'john', 'vcf_file', 'data.vcf.gz')
    #     metadata = os.path.join(self.ftp_box, 'upload', 'john', 'metadata.xlsx')
    #
    #     assert find_files_for(self.ftp_box) == ([vcf_file], [], [metadata], [])
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

    def test_report(self):
        box = FtpDepositBox(1, 'john')
        box.report()