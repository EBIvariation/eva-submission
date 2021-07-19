import os
from unittest import TestCase

from eva_submission import ROOT_DIR
from eva_submission.xlsx.xlsx_parser_eva import XlsxWriter, EvaXlsxReader


class TestXlsxWriter(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    metadata_file = os.path.join(resources_folder, 'metadata.xlsx')
    metadata_copy_file = os.path.join(resources_folder, 'metadata_copy.xlsx')
    eva_xls_reader_conf = os.path.join(resources_folder, 'test_metadata_fields.yaml')

    def setUp(self):
        self.xls_writer = XlsxWriter(self.metadata_file, self.eva_xls_reader_conf)
        self.reader = EvaXlsxReader(self.metadata_file)

    def tearDown(self):
        pass
        if os.path.exists(self.metadata_copy_file):
            os.remove(self.metadata_copy_file)

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
        self.xls_writer.save(self.metadata_copy_file)
        reader = EvaXlsxReader(self.metadata_copy_file)
        assert len(reader.samples) == 100
        assert reader.samples[0]['Sample Accession'] == 'SABCDEF1'

    def test_set_rows(self):
        self.xls_writer.active_worksheet = 'Sample'
        rows = [
            {
                'Analysis Alias': 'GAE',
                'Sample ID': 'S' + str(sample_num),
                'Sample Accession': 'SABCDEFGHIJKLMNOP' + str(sample_num),
            } for sample_num in range(1, 51)
        ]
        self.xls_writer.set_rows(rows, empty_remaining_rows=True)
        self.xls_writer.save(self.metadata_copy_file)
        reader = EvaXlsxReader(self.metadata_copy_file)
        assert len(reader.samples) == 50
        assert reader.samples[0]['Sample Accession'] == 'SABCDEFGHIJKLMNOP1'
