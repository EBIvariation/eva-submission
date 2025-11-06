import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config


class TestEloadValidation(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.validation = EloadValidation(2)
        self.sv_validation = EloadValidation(70)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.validation.eload_cfg.content)
        self.original_sv_cfg = deepcopy(self.sv_validation.eload_cfg.content)

    def tearDown(self):
        self.validation.eload_cfg.content = self.original_cfg
        self.sv_validation.eload_cfg.content = self.original_sv_cfg

    def test_parse_sv_check_log(self):
        sv_check_log = os.path.join(self.resources_folder, 'validations', 'sv_check.log')
        assert self.validation.parse_sv_check_log(sv_check_log) == 33

    def test_update_cli_report_with_new_path(self):
        report_file = os.path.join(self.resources_folder, 'tmp_report.txt')
        old_path = '/path/to/old/dir'
        new_path = '/path/to/new/dir'
        with open(report_file, 'w') as f:
            f.writelines([
                'line does not match\n',
                f'line does match: {old_path}/80/154577d76dbd9e2e05b80842ec410d/validation_output/important_report.txt\n',
                f'line does match: {old_path}/validation_output/important_report.txt'
            ])
        self.validation._update_cli_report_with_new_path(report_file, old_path, new_path)
        with open(report_file) as f:
            assert f.readlines() == [
                'line does not match\n',
                f'line does match: {new_path}/important_report.txt\n',
                f'line does match: {new_path}/important_report.txt'
            ]


    def test_report(self):
        expected_report = '''Validation performed on 2020-11-01 10:37:54.755607
eva-sub-cli:
  VCF checks: PASS
  Assembly checks: PASS
  Metadata check: PASS
  Sample check: PASS
Structural variant check: PASS
Naming convention check: PASS
----------------------------------

eva-sub-cli:

VALIDATION REPORT
eva-sub-cli v0.4.9
-
PROJECT SUMMARY
General details about the project
	Project Title: My cool project
	Validation Date: 2020-11-01 10:37:54
	Submission Directory: eloads/ELOAD_2
	Files mapping:
		---
		VCF File: input_passed.vcf
		Fasta File: metadata_asm_match.fa
		Analysis: A
-
METADATA VALIDATION RESULTS
Ensures that required fields are present and values are formatted correctly.
For requirements, please refer to the EVA website (https://www.ebi.ac.uk/eva/?Submit-Data).
    ✔ Metadata validation check
-
VCF VALIDATION RESULTS
Checks whether each file is compliant with the VCF specification (http://samtools.github.io/hts-specs/VCFv4.4.pdf).
Also checks whether the variants' reference alleles match against the reference assembly.
	input_passed.vcf
		✔ Assembly check: 247/247 (100.0%)
		✔ VCF check: 0 critical errors, 0 non-critical errors
-
SAMPLE NAME CONCORDANCE CHECK
Checks whether information in the metadata is concordant with that contained in the VCF files, in particular sample names.
	✔ Analysis A: Sample names in metadata match with those in VCF files
-
REFERENCE GENOME INSDC CHECK
Checks that the reference sequences in the FASTA file used to call the variants are accessioned in INSDC.
Also checks if the reference assembly accession in the metadata matches the one determined from the FASTA file.
	metadata_asm_match.fa
		✔ All sequences are INSDC accessioned.
		✔ Analysis A: Assembly accession in metadata is compatible

----------------------------------

Structural variant check:
  * test.vcf has structural variants

----------------------------------

Naming convention check:
  * Naming convention: enaSequenceName
    * test.vcf: enaSequenceName

----------------------------------
'''
        print(self.validation.report())
        with patch('builtins.print') as mprint:
            self.validation.report()

        mprint.assert_called_once_with(expected_report)

    def test_mark_valid_files_and_metadata(self):
        assert self.validation.eload_cfg.query('validation', 'valid') is None
        self.validation.mark_valid_files_and_metadata()
        # Check that the normalised file was picked up instead of the original file
        expected = {'analyses': {'ELOAD_2_analysis_alias': {'vcf_files': ['test.vcf']}},
                    'metadata_spreadsheet': '/path/to/the/spreadsheet'}
        assert self.validation.eload_cfg.query('validation', 'valid') == expected

    def test_update_config_with_cli_results(self):
        self.validation._update_config_with_cli_results(
            os.path.join(self.validation._get_dir('eva_sub_cli'), 'validation_results.yaml'))
        expected_aggregation = {
            'ELOAD_2_Analysis A': None,
            'ELOAD_2_Analysis B': 'basic',
        }
        assert self.validation.eload_cfg.query('validation', 'aggregation_check', 'analyses') == expected_aggregation
        assert self.validation.eload_cfg.query('validation', 'vcf_check')['pass'] == False
        assert self.validation.eload_cfg.query('validation', 'assembly_check')['pass'] == True
        assert self.validation.eload_cfg.query('validation', 'metadata_check')['pass'] == True
        assert self.validation.eload_cfg.query('validation', 'sample_check')['pass'] == True

    def test_set_validation_task_result_valid(self):
        self.validation.eload_cfg.set('validation', 'vcf_check', 'pass', value=False)
        self.validation.eload_cfg.set('validation', 'assembly_check', 'pass', value=True)
        del self.validation.eload_cfg['validation']['metadata_check']

        validation_tasks = ['vcf_check', 'assembly_check', 'metadata_check']
        self.validation.set_validation_task_result_valid(validation_tasks)

        assert self.validation.eload_cfg.query('validation', 'vcf_check')['pass'] == False
        assert self.validation.eload_cfg.query('validation', 'vcf_check')['forced'] == True

        assert self.validation.eload_cfg.query('validation', 'assembly_check')['pass'] == True
        assert 'forced' not in self.validation.eload_cfg.query('validation', 'assembly_check')

        assert 'pass' not in self.validation.eload_cfg.query('validation', 'metadata_check')
        assert self.validation.eload_cfg.query('validation', 'metadata_check')['forced'] == True

