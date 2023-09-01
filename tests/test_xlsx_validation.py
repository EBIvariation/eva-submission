import datetime
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader
from eva_submission.xlsx.xlsx_validation import EvaXlsxValidator


class TestEvaXlsValidator(TestCase):

    def setUp(self) -> None:
        brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        metadata_file = os.path.join(brokering_folder, 'metadata_sheet2.xlsx')
        metadata_file_fail = os.path.join(brokering_folder, 'metadata_sheet_fail.xlsx')
        self.validator = EvaXlsxValidator(metadata_file)
        self.validator_fail = EvaXlsxValidator(metadata_file_fail)

        metadata_file_wrong_sc_name = os.path.join(brokering_folder, 'metadata_wrong_scientific_name.xlsx')
        self.metadata_file_wrong_sc_name_copy = os.path.join(brokering_folder, 'metadata_wrong_scientific_name_copy.xlsx')
        shutil.copy(metadata_file_wrong_sc_name, self.metadata_file_wrong_sc_name_copy)
        self.validator_sc_name = EvaXlsxValidator(self.metadata_file_wrong_sc_name_copy)

    def tearDown(self) -> None:
        os.remove(self.metadata_file_wrong_sc_name_copy)

    def test_cerberus_validation(self):
        self.validator.cerberus_validation()
        self.assertEqual(self.validator.error_list, [])

    def test_cerberus_validation_failure(self):
        self.validator_fail.cerberus_validation()
        expected_errors = ['In Sheet Analysis, Row 4, field Analysis Alias: null value not allowed']
        self.assertEqual(self.validator_fail.error_list, expected_errors)

    def test_complex_validation(self):
        self.validator.complex_validation()
        assert self.validator.error_list == []

    def test_complex_validation_failure(self):
        self.validator_fail.complex_validation()
        expected_errors = [
            'Check Analysis Alias vs Samples: GAE2,None present in Analysis Alias not in Samples',
            'Check Analysis Alias vs Files: GAE2,None present in Analysis Alias not in Files',
            'In row 102, collection_date is not a date or "not provided": it is set to "Date of collection"'
        ]
        self.assertEqual(self.validator_fail.error_list, expected_errors)

    def test_validate(self):
        with patch('eva_submission.xlsx.xlsx_validation.get_scientific_name_from_ensembl') as m_sci_name:
            m_sci_name.return_value = 'Homo sapiens'
            self.validator.validate()
        assert self.validator.error_list == []

    def test_correct_scientific_name_in_metadata(self):
        reader_before_modification = EvaXlsxReader(self.metadata_file_wrong_sc_name_copy)
        scientific_name_list = [sample['Scientific Name'] for sample in reader_before_modification.samples]
        assert len(scientific_name_list) == 100
        assert len([s for s in scientific_name_list if s == 'Homo sapiens']) == 80
        assert len([s for s in scientific_name_list if s == 'Homo Sapiens']) == 10
        assert len([s for s in scientific_name_list if s == 'HS']) == 10

        with patch('eva_submission.xlsx.xlsx_validation.get_scientific_name_from_ensembl') as m_sci_name:
            m_sci_name.return_value = 'Homo sapiens'
            self.validator_sc_name.validate()
        assert self.validator_sc_name.error_list == ['In Samples, Taxonomy 9606 and scientific name HS are inconsistent']

        reader_after_modification = EvaXlsxReader(self.metadata_file_wrong_sc_name_copy)
        scientific_name_list = [sample['Scientific Name'] for sample in reader_after_modification.samples]
        assert len(scientific_name_list) == 100
        assert len([s for s in scientific_name_list if s == 'Homo sapiens']) == 90
        assert len([s for s in scientific_name_list if s == 'Homo Sapiens']) == 0
        assert len([s for s in scientific_name_list if s == 'HS']) == 10

    def test_check_date(self):
        assert self.validator.error_list == []
        row = {"row_num": 1, "collection_date": 'not provided'}
        self.validator.check_date(row, 'collection_date', required=True)
        assert self.validator.error_list == []

        row = {"row_num": 1, "collection_date": datetime.date(year=2019, month=6, day=8)}
        self.validator.check_date(row, 'collection_date', required=True)
        assert self.validator.error_list == []

        row = {"row_num": 1, "collection_date": '2019-06-08'}
        self.validator.check_date(row, 'collection_date', required=True)
        assert self.validator.error_list == []

        row = {"row_num": 1, "collection_date": '2019-06-08,2019-06-09'}
        self.validator.check_date(row, 'collection_date', required=True)
        assert self.validator.error_list == [
            'In row 1, collection_date is not a date or "not provided": it is set to "2019-06-08,2019-06-09"'
        ]
