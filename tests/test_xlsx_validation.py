import os
from unittest import TestCase

from eva_submission.xlsx.xlsx_validation import EvaXlsxValidator


class TestEvaXlsValidator(TestCase):

    def setUp(self) -> None:
        brokering_folder = os.path.join(os.path.dirname(__file__), 'resources', 'brokering')
        metadata_file = os.path.join(brokering_folder, 'metadata_sheet2.xlsx')
        self.validator = EvaXlsxValidator(metadata_file)

    def test_cerberus_validation(self):
        self.validator.cerberus_validation()
        print(self.validator.errors)

    def test_complex_validation(self):
        self.validator.complex_validation()
        print(self.validator.errors)

    def test_validate(self):
        self.validator.validate()
        print(self.validator.errors)
