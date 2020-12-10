import os
from unittest import TestCase

from eva_submission.xls_parser import XLSReader
from eva_submission.xls_parser_eva import EVAXLSReader


class TestEVAXLSReader(TestCase):

    metadata_file = os.path.join(os.path.dirname(__file__), 'resources', 'metadata.xlsx')

    def test_get_all_rows(self):
        reader = EVAXLSReader(self.metadata_file)
        rows = reader._get_all_rows('Analysis')
        assert len(rows) == 1
        assert rows[0]['Analysis Title'] == 'Greatest analysis ever'


class TestXLSReader(TestCase):

    metadata_file = os.path.join(os.path.dirname(__file__), 'resources', 'metadata.xlsx')
    eva_xls_reader_conf = os.path.join(os.path.dirname(__file__), 'resources', 'test_metadata_fields.yaml')

    def setUp(self):
        self.xls_reader = XLSReader(self.metadata_file, self.eva_xls_reader_conf)

    def test_valid_worksheets(self):
        worksheets = self.xls_reader.valid_worksheets()
        assert isinstance(worksheets, list)
        assert set(worksheets) == {'Project', 'Sample', 'Analysis'}

    def test_get_valid_conf_keys(self):
        worksheets = self.xls_reader.valid_worksheets()
        assert isinstance(worksheets, list)
        assert set(worksheets) == {'Project', 'Sample', 'Analysis'}

    def test_next_row(self):
        self.xls_reader.active_worksheet = 'Sample'
        row = self.xls_reader.next()
        assert isinstance(row, dict)
        assert row == {
            'Analysis Alias': 'GAE',
            'Sample Accession': None,
            'Sample ID': None,
            'Sample Name': 'S1',
            'Sampleset Accession': None,
            'Title': 'Sample 1',
            'row_num': 4
        }

        self.xls_reader.active_worksheet = 'Project'
        row = self.xls_reader.next()
        assert isinstance(row, dict)
        assert row == {
            'Project Title': 'Greatest project ever',
            'Project Alias': 'GPE',
            'Publication(s)': None,
            'Parent Project(s)': None,
            'Child Project(s)': None,
            'row_num': 2
        }

