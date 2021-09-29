import os
from unittest import TestCase

from eva_submission.eload_utils import check_existing_project, detect_vcf_aggregation


class TestEloadUtils(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def test_check_existing_project(self):
        assert check_existing_project('PRJ') is False
        assert check_existing_project('PRJEB42148') is True

    def test_detect_vcf_aggregation(self):
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_basic_aggregation.vcf')
        ) == 'basic'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_no_aggregation.vcf')
        ) == 'none'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_undetermined_aggregation.vcf')
        ) is None




