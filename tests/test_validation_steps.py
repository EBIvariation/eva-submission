import os
from copy import deepcopy
from unittest import TestCase
import yaml

from eva_submission import ROOT_DIR
from eva_submission.eload_validation import EloadValidation
from eva_submission.steps.structural_variant_detection import detect_structural_variant
from eva_submission.submission_config import load_config


class TestValidationSteps(TestCase):
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

    # def test_detect_structural_variant(self):
    #     detect_structural_variant()
    #
    #     self.sv_validation._detect_structural_variant()
    #     self.sv_validation.eload_cfg.write()
    #     with open(self.sv_validation.config_path, 'r') as config_file:
    #         config_data = yaml.safe_load(config_file)
    #         self.assertDictEqual(config_data['validation']['structural_variant_check']['files'],
    #         {'test1.vcf': {'has_structural_variant': True}, 'test2.vcf.gz': {'has_structural_variant': False}, 'test3.vcf': {'has_structural_variant': True}, 'test4.vcf': {'has_structural_variant': True}})
