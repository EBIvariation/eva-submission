import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.sample_utils import get_samples_from_vcf

class TestSampleChecker(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def test_get_samples_from_vcf(self):
        assert get_samples_from_vcf(os.path.join(self.resources_folder, 'test.vcf')) == ['S1']

