import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.steps.structural_variant_detection import detect_structural_variant


class TestValidationSteps(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')


    def test_detect_structural_variant(self):
        vcf_file = os.path.join(self.resources_folder, 'vcf_files', 'file_structural_variants.vcf')
        output_vcf = os.path.join(self.resources_folder, 'vcf_files', 'output.vcf')
        detect_structural_variant(vcf_file, output_vcf)
