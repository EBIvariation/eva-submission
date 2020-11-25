import os
from unittest import TestCase

from eva_submission.eload_submission import EloadValidation
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
        self.validation.report()

