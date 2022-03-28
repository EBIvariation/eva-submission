import os
from unittest import TestCase
from unittest.mock import patch

from eva_submission.ena_retrieval import files_from_ena, remove_file_from_analysis, \
    difference_evapro_file_set_with_ena_for_analysis, get_file_id_from_md5, insert_file_into_evapro, \
    insert_file_analysis_into_evapro
from eva_submission.submission_config import load_config


class TestEnaRetrieval(TestCase):
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)

    def test_files_from_ena(self):
        expected_files = {
            'ERZ468492': [
                {'filename': 'ERZ468/ERZ468492/OMIA001271_9913_2.vcf.gz.tbi', 'analysis_accession': 'ERZ468492',
                 'filetype': 'tabix', 'md5': '07f98ac44d6d6f1453d40dc61f29ecec'},
                {'filename': 'ERZ468/ERZ468492/OMIA001271_9913_2.vcf.gz',  'analysis_accession': 'ERZ468492',
                 'filetype': 'vcf', 'md5': '5118c4d13159750f476005d64fc27829'}
            ]
        }
        assert files_from_ena('ERZ468492') == expected_files

    def test_get_file_id_from_md5(self):
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
             patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = [('file_id',)]
            assert get_file_id_from_md5('07f98ac44d6d6f1453d40dc61f29ecec') == 'file_id'
            assert m_get_results.mock_calls[0][1][1] == \
                   "select file_id from file where file_md5='07f98ac44d6d6f1453d40dc61f29ecec'"

    def test_get_file_id_from_md5_duplicates(self):
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
             patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = [('file_id1',), ('file_id2',)]
            with self.assertRaises(ValueError):
                get_file_id_from_md5('07f98ac44d6d6f1453d40dc61f29ecec')

    def test_insert_file_into_evapro(self):
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.execute_query') as m_execute_query, \
                patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = [('file_id1',)]
            file_id = insert_file_into_evapro(
                {'filename': 'analysis_id/test.vcf.gz', 'md5': '07f98ac44d6d6f1453d40dc61f29ecec'}
            )
            assert file_id == 'file_id1'
            expected_query = ("insert into file (filename, file_md5, file_type, file_class, file_version, is_current, file_location, ftp_file) "
                              "values ('test.vcf.gz', '07f98ac44d6d6f1453d40dc61f29ecec', 'vcf', 'submitted', 1, 1, 'scratch_folder', 'ftp.sra.ebi.ac.uk/vol1/analysis_id/test.vcf.gz')")
            assert m_execute_query.mock_calls[0][1][1] == expected_query
            expected_query = 'update file set ena_submission_file_id=file_id1 where file_id=file_id1'
            assert m_execute_query.mock_calls[1][1][1] == expected_query

    def test_insert_file_analysis_into_evapro(self):
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.execute_query') as m_execute_query:
            insert_file_analysis_into_evapro({'file_id': 1234, 'analysis_accession': 'analysis1'})
            expected_query = "insert into analysis_file (ANALYSIS_ACCESSION,FILE_ID) values (1234, 'analysis1')"
            assert m_execute_query.mock_calls[0][1][1] == expected_query

    def test_remove_file_from_analysis(self):
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.execute_query') as m_execute_query:
            remove_file_from_analysis({'file_id': 1234, 'analysis_accession': 'analysis1'})
            expected_query = "delete from analysis_file where file_id=1234 and analysis_accession='analysis1'"
            assert m_execute_query.mock_calls[0][1][1] == expected_query

    def test_difference_evapro_file_set_with_ena_for_analysis_no_diff(self):
        analysis_accession = 'analysis1'
        ena_list_of_file_dicts = [
            {'filename': 'analysis_id/test.vcf.gz', 'md5': '07f98ac44d6d6f1453d40dc61f29ecec'}
        ]
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = [('test.vcf.gz', '07f98ac44d6d6f1453d40dc61f29ecec')]
            assert difference_evapro_file_set_with_ena_for_analysis(analysis_accession, ena_list_of_file_dicts) \
                == ([], [])

    def test_difference_plus_ena(self):
        analysis_accession = 'analysis1'
        ena_list_of_file_dicts = [
            {'filename': 'analysis_id/test.vcf.gz', 'md5': '07f98ac44d6d6f1453d40dc61f29ecec'}
        ]
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = []
            assert difference_evapro_file_set_with_ena_for_analysis(analysis_accession, ena_list_of_file_dicts) \
                   == (ena_list_of_file_dicts, [])

    def test_difference_plus_eva(self):
        analysis_accession = 'analysis1'
        ena_list_of_file_dicts = [
            {'filename': 'analysis_id/test.vcf.gz', 'md5': '07f98ac44d6d6f1453d40dc61f29ecec'}
        ]
        with patch('eva_submission.ena_retrieval.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.ena_retrieval.get_all_results_for_query') as m_get_results:
            m_get_results.return_value = [
                ('test.vcf.gz', '07f98ac44d6d6f1453d40dc61f29ecec'),
                ('test2.vcf.gz', '8dhks0fhhwu83h99sk94762hs889sj83')
            ]
            assert difference_evapro_file_set_with_ena_for_analysis(analysis_accession, ena_list_of_file_dicts) \
                   == ([], [{'filename': 'test2.vcf.gz', 'md5': '8dhks0fhhwu83h99sk94762hs889sj83'}])

