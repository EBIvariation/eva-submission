import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from eva_vcf_merge.detect import MergeType
from eva_vcf_merge.merge import VCFMerger

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

    def test_report(self):
        expected_report = '''Validation performed on 2020-11-01 10:37:54.755607
eva-sub-cli: PASS
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

VCF merge:
  Merge types:
  * ELOAD_2_a1: horizontal

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

    def test_detect_and_optionally_merge(self):
        original_content = deepcopy(self.validation.eload_cfg.content)
        analysis_alias = 'ELOAD_2_alias'
        valid_files = ['file1', 'file2']
        merged_files = {analysis_alias: 'merged.vcf.gz'}
        self.validation.eload_cfg.set('validation', 'valid', 'analyses', analysis_alias, 'vcf_files', value=valid_files)

        with patch('eva_submission.eload_validation.detect_merge_type', return_value=MergeType.HORIZONTAL), \
                patch.object(VCFMerger, 'horizontal_merge', return_value=merged_files):
            # Should detect merge type but not actually merge
            self.validation.detect_and_optionally_merge(False)
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'merge_type', analysis_alias),
                MergeType.HORIZONTAL.value
            )
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'valid', 'analyses', analysis_alias, 'vcf_files'),
                valid_files
            )
            # Should perform the merge
            self.validation.detect_and_optionally_merge(True)
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'valid', 'analyses', analysis_alias, 'vcf_files'),
                ['merged.vcf.gz']
            )
        self.validation.eload_cfg.content = original_content

    def test_merge_multiple_analyses(self):
        valid_files = {
            'horizontal': ['h1', 'h2'],
            'vertical': ['v1', 'v2'],
            'neither': ['n1', 'n2']
        }
        detections = [MergeType.HORIZONTAL, MergeType.VERTICAL, None]
        horiz_merged_files = {'horizontal': 'h.vcf.gz'}
        vert_merged_files = {'vertical': 'v.vcf.gz'}
        for analysis_alias, vcf_files in valid_files.items():
            self.validation.eload_cfg.set('validation', 'valid', 'analyses',
                                          analysis_alias, 'vcf_files', value=vcf_files)

        with patch('eva_submission.eload_validation.detect_merge_type', side_effect=detections), \
                patch.object(VCFMerger, 'horizontal_merge', return_value=horiz_merged_files), \
                patch.object(VCFMerger, 'vertical_merge', return_value=vert_merged_files):
            self.validation.detect_and_optionally_merge(True)
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'valid', 'analyses', 'horizontal', 'vcf_files'),
                ['h.vcf.gz']
            )
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'valid', 'analyses', 'vertical', 'vcf_files'),
                ['v.vcf.gz']
            )
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'valid', 'analyses', 'neither', 'vcf_files'),
                ['n1', 'n2']
            )

    def test_merge_multiple_analyses_same_name(self):
        valid_files = {
            'a!': ['h1', 'h2'],
            'a@': ['v1', 'v2'],
            'a2': ['n1', 'n2']
        }
        detections = [MergeType.HORIZONTAL, MergeType.VERTICAL, None]
        analyses_dict = {
            analysis_alias: {'vcf_files': vcf_files}
            for analysis_alias, vcf_files in valid_files.items()
        }
        self.validation.eload_cfg.set('validation', 'valid', 'analyses', value=analyses_dict)

        with patch('eva_submission.eload_validation.detect_merge_type', side_effect=detections):
            self.validation.detect_and_optionally_merge(True)
            # Valid files should be unchanged even though merge is detected
            self.assertEqual(self.validation.eload_cfg.query('validation', 'valid', 'analyses'), analyses_dict)
            self.assertEqual(
                self.validation.eload_cfg.query('validation', 'merge_errors'),
                ['Analysis aliases not valid as unique merged filenames']
            )

    def test_mark_valid_files_and_metadata(self):
        assert self.validation.eload_cfg.query('validation', 'valid') is None
        self.validation.mark_valid_files_and_metadata(merge_per_analysis=False)
        # Check that the normalised file was picked up instead of the original file
        expected = {'analyses': {'ELOAD_2_analysis_alias': {'vcf_files': ['test.vcf']}},
                    'metadata_spreadsheet': '/path/to/the/spreadsheet'}
        assert self.validation.eload_cfg.query('validation', 'valid') == expected

    def test_update_config_with_cli_results(self):
        self.validation._update_config_with_cli_results(
            os.path.join(self.validation._get_dir('eva_sub_cli'), 'validation_results.yaml'))
        expected_aggregation = {
            'Analysis A': None,
            'Analysis B': 'none',
            'Analysis C': 'basic',
        }
        assert self.validation.eload_cfg.query('validation', 'aggregation_check', 'analyses') == expected_aggregation
        assert self.validation.eload_cfg.query('validation', 'eva_sub_cli')['pass'] == False
