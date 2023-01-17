import os
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.steps.structural_variant_detection import detect_structural_variant


class TestValidationSteps(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    vcf_file = os.path.join(resources_folder, 'vcf_files', 'file_structural_variants.vcf')
    output_vcf = os.path.join(resources_folder, 'vcf_files', 'output_structural_variants.vcf')

    def tearDown(self) -> None:
        os.remove(self.output_vcf)

    def test_detect_structural_variant(self):

        assert not os.path.exists(self.output_vcf)
        with patch('builtins.print') as mprint:
            detect_structural_variant(self.vcf_file, self.output_vcf)
        mprint.assert_called_once_with('1 lines containing structural variants')
        assert os.path.exists(self.output_vcf)

