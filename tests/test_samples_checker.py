import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.samples_checker import get_samples_from_vcf, get_sample_names, compare_names_in_files_and_samples, \
    compare_spreadsheet_and_vcf


class TestSampleChecker(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def test_get_samples_from_vcf(self):
        assert get_samples_from_vcf(os.path.join(self.resources_folder, 'test.vcf')) == ['S1']

    def test_get_sample_names(self):
        assert get_sample_names([{'Sample Name': 'S1'}, {'Sample ID': 'S2'}, {'Analysis': ''}]) == ['S1', 'S2']

    def test_compare_names_in_files_and_samples(self):
        assert compare_names_in_files_and_samples(
            [os.path.join(self.resources_folder, 'test.vcf')],
            [{'Sample Name': 'S1'}],
            'A1'
        ) == (False, [], [])

        assert compare_names_in_files_and_samples(
            [os.path.join(self.resources_folder, 'test.vcf')],
            [{'Sample Name': 'S1'}, {'Sample Name': 'S2'}],
            'A1'
        ) == (True, [], ['S2'])

    def test_compare_spreadsheet_and_vcf(self):
        metadata_file = os.path.join(self.resources_folder, 'metadata_2_analysis_same_samples.xlsx')
        vcf_dir = os.path.join(self.resources_folder, 'vcf_dir')
        results_per_analysis_alias = compare_spreadsheet_and_vcf(metadata_file, vcf_dir)
        assert results_per_analysis_alias == {'GAE': (False, [], []), 'GAE2': (False, [], [])}
