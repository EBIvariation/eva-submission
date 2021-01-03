import os
from unittest import TestCase

from eva_submission.xlsx.xlsx_parser_eva import XLSWriter, EVAXLSReader


class TestXLSWriter(TestCase):

    metadata_file = os.path.join(os.path.dirname(__file__), 'resources', 'metadata.xlsx')
    eva_xls_reader_conf = os.path.join(os.path.dirname(__file__), 'resources', 'test_metadata_fields.yaml')

    def setUp(self):
        self.xls_writer = XLSWriter(self.metadata_file, self.eva_xls_reader_conf)
        self.reader = EVAXLSReader(self.metadata_file)

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

