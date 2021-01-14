import os
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.submission_config import load_config
from eva_submission.submission_in_ftp import FtpDepositBox


class TestFtpDepositBox(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)

    def test_report(self):
        # Mock the stat function so that it returns a consistent values
        with patch('os.stat') as m_stat, patch('builtins.print') as mprint:
            m_stat.return_value.st_size = 100
            m_stat.return_value.st_mtime = 1604000000

            box = FtpDepositBox(1, 'john')
            box.report()

            expected_report = """#############################
ftp box: tests/resources/ftpboxes/eva-box-01/upload/john
last modified: 2020-10-29 19:33:20
size: 300 Bytes
-----
number of vcf files: 1
last modified: 2020-10-29 19:33:20
size: 100 Bytes
-----
number of metadata spreadsheet: 1
metadata file path: tests/resources/ftpboxes/eva-box-01/upload/john/metadata.xlsx
last modified: 2020-10-29 19:33:20
Project title: Greatest project ever
Number of analysis: 1
Reference sequence: GCA_000001405.1
Number of sample: 100
#############################"""
        mprint.assert_called_with(expected_report)

