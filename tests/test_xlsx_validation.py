import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.xlsx.xlsx_validation import EvaXlsxValidator


class TestEvaXlsValidator(TestCase):

    def setUp(self) -> None:
        brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        metadata_file = os.path.join(brokering_folder, 'metadata_sheet2.xlsx')
        metadata_file_fail = os.path.join(brokering_folder, 'metadata_sheet_fail.xlsx')
        self.validator = EvaXlsxValidator(metadata_file)
        self.validator_fail = EvaXlsxValidator(metadata_file_fail)

    def test_cerberus_validation(self):
        self.validator.cerberus_validation()
        self.assertEqual(self.validator.error_list, [])

        self.validator_fail.cerberus_validation()
        expected_errors = ['In Sheet Analysis, Row 4, field Analysis Alias: null value not allowed']
        self.assertEqual(self.validator_fail.error_list, expected_errors)

    def test_complex_validation(self):
        self.validator.complex_validation()
        assert self.validator.error_list == []
        self.validator_fail.complex_validation()
        expected_errors = [
            'Check Analysis Alias vs Samples: GAE2,None present in Analysis Alias not in Samples',
            'Check Analysis Alias vs Files: GAE2,None present in Analysis Alias not in Files'
        ]
        self.assertEqual(self.validator_fail.error_list, expected_errors)

    def test_validate(self):
        self.validator.validate()
        assert self.validator.error_list == []
