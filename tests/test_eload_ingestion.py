import glob
import os
import shutil
import subprocess
from copy import deepcopy
from unittest import TestCase, mock
from unittest.mock import patch

from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config


class TestEloadIngestion(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        with patch('eva_submission.eload_ingestion.get_mongo_uri_for_eva_profile', autospec=True):
            self.eload = EloadIngestion(33)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

    def tearDown(self):
        projects = glob.glob(os.path.join(self.resources_folder, 'projects', 'PRJEB12345'))
        for proj in projects:
            shutil.rmtree(proj)
        ingest_csv = os.path.join(self.eload.eload_dir, 'vcf_files_to_ingest.csv')
        if os.path.exists(ingest_csv):
            os.remove(ingest_csv)
        self.eload.eload_cfg.content = self.original_cfg

    def _patch_get_dbname(self, db_name):
        m_get_db_name = patch(
            'eva_submission.eload_ingestion.resolve_variant_warehouse_db_name',
            autospec=True,
            return_value=db_name
        )
        return m_get_db_name

    def _patch_metadata_handle(self):
        return patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True)

    def _patch_mongo_database(self, collection_names=None):
        mongodb_instance = mock.Mock()
        if collection_names:
            mongodb_instance.get_collection_names.return_value = collection_names
        else:
            mongodb_instance.get_collection_names.return_value = []
        return patch('eva_submission.eload_utils.MongoDatabase', autospec=True, return_value=mongodb_instance)

    def test_check_brokering_done(self):
        self.eload.project_accession = None
        with self.assertRaises(ValueError):
            self.eload.check_brokering_done()
        del self.eload.eload_cfg.content['brokering']
        with self.assertRaises(ValueError):
            self.eload.check_brokering_done()

    def test_check_variant_db_no_creation(self):
        with self._patch_metadata_handle(), self._patch_get_dbname('eva_ecaballus_30'), \
             self._patch_mongo_database(collection_names=['col1']) as m_mongo:
            self.eload.check_variant_db()

            # Check the database name is correct and has been set in the config
            self.assertEqual(
                'eva_ecaballus_30',
                self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'db_name')
            )
            # Check the database already existed
            m_mongo.return_value.get_collection_names.assert_called_once_with()
            m_mongo.return_value.enable_sharding.assert_not_called()

    def test_check_variant_db_name_not_creatd(self):
        with self._patch_metadata_handle(), self._patch_get_dbname(None):
            # Database name cannot be retrieve or constructed raise error
            with self.assertRaises(ValueError):
                self.eload.check_variant_db()

    def test_check_variant_db_with_creation(self):
        with self._patch_metadata_handle(), self._patch_get_dbname('eva_ecaballus_30'), \
             self._patch_mongo_database(collection_names=[]) as m_mongo:

            self.eload.check_variant_db()
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'db_name'),
                'eva_ecaballus_30'
            )
            # Check the database was created
            m_mongo.return_value.get_collection_names.assert_called_once_with()
            m_mongo.return_value.enable_sharding.assert_called_once_with()
            m_mongo.return_value.shard_collections.assert_called_once()

    def test_load_from_ena(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_script_fails(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_ingest_all_tasks(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            browsable_files = [(1, 'ERA', 'filename_1', 'PRJ', 123), (2, 'ERA', 'filename_1', 'PRJ', 123)]
            m_get_results.side_effect = [
                [(391,)],                                # Check the assembly_set_id in update_assembly_set_in_analysis
                browsable_files,                         # insert_browsable_files files_query
                browsable_files,                         # insert_browsable_files find_browsable_files_query
                [(1, 'filename_1'), (2, 'filename_2')],  # update_files_with_ftp_path
                [('Test Study Name')],                   # get_study_name
                [(1, 'filename_1'), (2, 'filename_2')]   # update_loaded_assembly_in_browsable_files
            ]
            self.eload.ingest(1)

    def test_ingest_metadata_load(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            self.eload.ingest(tasks=['metadata_load'])

    def test_ingest_accession(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.return_value = [(1, 'filename_1'), (2, 'filename_2')]
            self.eload.ingest(
                instance_id=1,
                tasks=['accession']
            )
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/accession_params.yaml')
            )

    def test_ingest_variant_load(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = [[('Test Study Name')], [(1, 'filename_1'), (2, 'filename_2')]]
            self.eload.ingest(tasks=['variant_load'])
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/variant_load_params.yaml')
            )

    def test_insert_browsable_files(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.execute_query') as m_execute:
            m_get_results.side_effect = [
                [],                                      # files_query
                [],                                      # find_browsable_files_query
                [(1, 'ERA', 'filename_1', 'PRJ', 123),
                 (2, 'ERA', 'filename_1', 'PRJ', 123)],  # files_query
                [(1, 'ERA', 'filename_1', 'PRJ', 123),
                 (2, 'ERA', 'filename_1', 'PRJ', 123)]   # find_browsable_files_query
            ]
            self.eload.insert_browsable_files()
            m_execute.assert_called()

            # calling insert again doesn't execute anything
            m_execute.call_count = 0
            self.eload.insert_browsable_files()
            m_execute.assert_not_called()

    def test_insert_browsable_files_warning(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch.object(EloadIngestion, 'warning') as m_warning:
            m_get_results.side_effect = [
                [(1, 'ERA', 'filename_1', 'PRJ', 123),
                 (2, 'ERA', 'filename_1', 'PRJ', 123)],  # files_query
                [(1, 'ERA', 'filename_1', 'PRJ', 123),
                 (2, 'ERA', 'filename_1', 'PRJ', 234)],  # find_browsable_files_query
            ]
            assert m_warning.call_count == 0
            self.eload.insert_browsable_files()
            assert m_warning.call_count == 1

    def test_update_browsable_files_with_date(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.execute_query') as m_execute:
            self.eload.update_browsable_files_with_date()
            m_execute.assert_called()

    def test_update_files_with_ftp_path(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.execute_query') as m_execute:
            m_get_results.side_effect = [[(1, 'filename_1')], []]
            self.eload.update_files_with_ftp_path()
            m_execute.assert_called()

            # calling insert again fail because no files are present
            m_execute.call_count = 0
            with self.assertRaises(ValueError):
                self.eload.update_files_with_ftp_path()
            m_execute.assert_not_called()

    def get_mock_result_for_ena_date(self):
        return '''<?xml version="1.0" encoding="UTF-8"?>
            <?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
            <RECEIPT receiptDate="2021-04-19T18:37:45.129+01:00" submissionFile="SUBMISSION" success="true">
                 <ANALYSIS accession="ERZ999999" alias="MD" status="PRIVATE"/>
                 <PROJECT accession="PRJEB12345" alias="alias" status="PRIVATE" holdUntilDate="2021-01-01+01:00"/>
                 <SUBMISSION accession="ERA3972426" alias="alias"/>
                 <MESSAGES/>
                 <ACTIONS>RECEIPT</ACTIONS>
            </RECEIPT>'''

    def assert_vep_versions(self, vep_version, vep_cache_version, vep_species):
        ingest_csv = os.path.join(self.eload.eload_dir, 'vcf_files_to_ingest.csv')
        assert os.path.exists(ingest_csv)
        with open(ingest_csv, 'r') as f:
            rows = [l.strip().split(',') for l in f.readlines()][1:]
            self.assertEqual({r[-3] for r in rows}, {str(vep_version)})
            self.assertEqual({r[-4] for r in rows}, {str(vep_cache_version)})
            self.assertEqual({r[-2] for r in rows}, {str(vep_species)})

    def test_ingest_variant_load_vep_versions_found(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = [[('Test Study Name')], [(1, 'filename_1'), (2, 'filename_2')]]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            self.eload.ingest(tasks=['variant_load'])
            self.assert_vep_versions(100, 100, 'homo_sapiens')

    def test_ingest_variant_load_vep_versions_not_found(self):
        """
        If VEP cache version is not found but no exception is raised, we should proceed with variant load
        but skip annotation.
        """
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = [[('Test Study Name')], [(1, 'filename_1'), (2, 'filename_2')]]
            m_get_vep_versions.return_value = (None, None, None)
            self.eload.ingest(tasks=['variant_load'])
            self.assert_vep_versions('', '', '')

    def test_ingest_variant_load_vep_versions_error(self):
        """
        If getting VEP cache version raises an exception, we should stop the loading process altogether.
        """
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = [[('Test Study Name')], [(1, 'filename_1'), (2, 'filename_2')]]
            m_get_vep_versions.side_effect = ValueError()
            with self.assertRaises(ValueError):
                self.eload.ingest(tasks=['variant_load'])
            config_file = os.path.join(self.resources_folder, 'projects/PRJEB12345/variant_load_params.yaml')
            assert not os.path.exists(config_file)

    def test_ingest_annotation_only(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = [[('Test Study Name')], [(1, 'filename_1'), (2, 'filename_2')]]
            self.eload.ingest(tasks=['annotation'])
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/variant_load_params.yaml')
            )
