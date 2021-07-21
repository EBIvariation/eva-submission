import glob
import os
import shutil

from covid19_submission.steps.run_vcf_validator import run_vcf_validation
from eva_submission import ROOT_DIR
from unittest import TestCase


class TestRunVcfValidator(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'covid19_submission')
    vcf_files_folder = os.path.join(resources_folder, 'vcf_files')
    validator_test_run_folder = os.path.join(resources_folder, 'validator_run')

    def setUp(self) -> None:
        shutil.rmtree(self.validator_test_run_folder, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.validator_test_run_folder, ignore_errors=True)

    def test_vcf_validation_with_acceptable_errors(self):
        run_vcf_validation(vcf_file=f"{self.vcf_files_folder}/file1.vcf.gz", output_dir=self.validator_test_run_folder,
                           validator_binary="vcf_validator_linux")
        output_files = glob.glob(f"{self.validator_test_run_folder}/file1.vcf.gz.errors.*.*")
        self.assertEqual(2, len(output_files))

    def test_vcf_validation_with_unacceptable_errors(self):
        with self.assertRaises(SystemExit) as exit_exception:
            run_vcf_validation(vcf_file=f"{self.vcf_files_folder}/file_with_errors.vcf.gz",
                               output_dir=self.validator_test_run_folder, validator_binary="vcf_validator_linux")
        self.assertEqual(exit_exception.exception.args[0], "Unacceptable VCF validation errors found!")
