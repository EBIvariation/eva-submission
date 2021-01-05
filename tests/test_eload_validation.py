import os
from unittest import TestCase
from unittest.mock import patch

from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config


class TestEloadValidation(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.validation = EloadValidation(2)

    def test_parse_assembly_check_log(self):
        assembly_check_log = os.path.join(self.resources_folder, 'validations', 'failed_assembly_check.log')
        expected = (
            [" The assembly checking could not be completed: Contig '8' not found in assembly report"],
            1,
            0,
            0
        )
        assert self.validation.parse_assembly_check_log(assembly_check_log) == expected

    def test_parse_vcf_check_report(self):
        vcf_check_report = os.path.join(self.resources_folder, 'validations', 'failed_file.vcf.errors.txt')

        valid, error_list, nb_error, nb_warning = self.validation.parse_vcf_check_report(vcf_check_report)
        assert valid is False
        assert len(error_list) == 8
        assert nb_error == 8
        assert nb_warning == 1

    def test_report(self):
        expected_report = '''Validation performed on 2020-11-01 10:37:54.755607
Metadata check: PASS
VCF check: PASS
Assembly check: PASS
Sample names check: PASS
----------------------------------

Metadata check:
  * /path/to/spreadsheet: PASS
    - number of error: 0
    - error messages: 

----------------------------------

VCF check:
  * test.vcf: PASS
    - number of error: 0
    - number of warning: 2
    - first 10 errors: 
    - see report for detail: /path/to/report

----------------------------------

Assembly check:
  * test.vcf: PASS
    - number of error: 0
    - match results: 20/20
    - first 10 errors: 
    - see report for detail: /path/to/report

----------------------------------

Sample names check:
  * a1: PASS
    - Samples that appear in the VCF but not in the Metadata sheet: 
    - Samples that appear in the Metadata sheet but not in the VCF file(s): 

----------------------------------
'''
        with patch('builtins.print') as mprint:
            self.validation.report()
        mprint.assert_called_once_with(expected_report)



