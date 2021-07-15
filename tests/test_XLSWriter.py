import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.xlsx.xlsx_parser_eva import XlsxWriter, EvaXlsxReader


class TestXlsxWriter(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    metadata_file = os.path.join(resources_folder, 'metadata.xlsx')
    eva_xls_reader_conf = os.path.join(resources_folder, 'test_metadata_fields.yaml')

    def setUp(self):
        self.xls_writer = XlsxWriter(self.metadata_file, self.eva_xls_reader_conf)
        self.reader = EvaXlsxReader(self.metadata_file)

    def tearDown(self):
        if os.path.exists(os.path.join(self.resources_folder, 'metadata_copy.xlsx')):
            os.remove(os.path.join(self.resources_folder, 'metadata_copy.xlsx'))

    def test_edit_row(self):
        self.xls_writer.active_worksheet = 'Sample'
        for sample_num in range(1, 101):
            self.xls_writer.edit_row(
                {
                    'Analysis Alias': 'GAE',
                    'Sample ID': 'S' + str(sample_num),
                    'Sample Accession': 'SABCDEF' + str(sample_num),
                    'row_num': sample_num + 3
                },
                remove_when_missing_values=True
            )
        self.xls_writer.save(os.path.join(os.path.dirname(__file__), 'resources', 'metadata_copy.xlsx'))

    def test_set_rows(self):
        self.xls_writer.active_worksheet = 'Sample'
        rows = [
            {
                'Analysis Alias': 'GAE',
                'Sample ID': 'S' + str(sample_num),
                'Sample Accession': 'SABCDEFGHIJKLMNOP' + str(sample_num),
            } for sample_num in range(1, 101)
        ]
        self.xls_writer.set_rows(rows)
        self.xls_writer.save(os.path.join(os.path.dirname(__file__), 'resources', 'metadata_copy.xlsx'))

