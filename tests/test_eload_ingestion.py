import glob
import os
import shutil
import subprocess
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

    def tearDown(self):
        projects = glob.glob(os.path.join(self.resources_folder, 'projects', 'PRJEB12345'))
        for proj in projects:
            shutil.rmtree(proj)

    def _mock_mongodb_client(self):
        m_db = mock.Mock()
        m_db.list_database_names = mock.Mock(return_value=[
            'eva_ecaballus_30',
            'eva_hsapiens_grch38'
        ])
        return m_db

    def test_check_variant_db(self):
        with patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.get_variant_warehouse_db_name_from_assembly_and_taxonomy',
                      autospec=True) as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_results.return_value = 'eva_ecaballus_30'
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()

            self.eload.check_variant_db()
            self.assertEqual(
                'eva_ecaballus_30',
                self.eload.eload_cfg.query('ingestion', 'database', 'db_name')
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'exists')

    def test_check_variant_db_not_in_evapro(self):
        with patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.get_variant_warehouse_db_name_from_assembly_and_taxonomy',
                      autospec=True) as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_results.return_value = None
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            with self.assertRaises(ValueError):
                self.eload.check_variant_db()

    def test_check_variant_db_name_provided(self):
        with patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.check_variant_db(db_name='eva_hsapiens_grch38')
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'db_name'),
                'eva_hsapiens_grch38'
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'exists')

    def test_check_variant_db_missing(self):
        with patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()

            with self.assertRaises(ValueError):
                self.eload.check_variant_db(db_name='eva_fcatus_90')
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'db_name'),
                'eva_fcatus_90'
            )
            assert not self.eload.eload_cfg.query('ingestion', 'database', 'exists')

    def test_load_from_ena(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_no_project_accession(self):
        self.eload.project_accession = None
        with self.assertRaises(ValueError):
            self.eload.load_from_ena()

    def test_load_from_ena_script_fails(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_merge_vcfs(self):
        expected = ['tests/resources/projects/PRJEB12345/31_merged/PRJEB12345_merged.vcf.gz']
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            self.assertEqual(expected, sorted(str(x) for x in self.eload.merge_vcfs()))

    def test_merge_vcfs_duplicate_names(self):
        expected = [
            'tests/resources/projects/PRJEB12345/30_eva_valid/test1.vcf.gz',
            'tests/resources/projects/PRJEB12345/30_eva_valid/test2.vcf.gz'
        ]
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(
                1, 'some command',
                'Error: Duplicate sample names (HG002), use --force-samples to proceed anyway.'
            )
            self.assertEqual(expected, sorted(str(x) for x in self.eload.merge_vcfs()))

    def test_merge_vcfs_error(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.merge_vcfs()
            m_execute.assert_called_once()

    def test_ingest_all_tasks(self):
        with patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            self.eload.ingest('NONE', 1, 82, 82, db_name='eva_hsapiens_grch38')

    def test_ingest_metadata_load(self):
        with patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.ingest(tasks=['metadata_load'], db_name='eva_hsapiens_grch38')

    def test_ingest_accession(self):
        with patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.ingest(
                aggregation='NONE',
                instance_id=1,
                tasks=['accession'],
                db_name='eva_hsapiens_grch38'
            )

    def test_ingest_variant_load_no_aggregation(self):
        with patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            self.eload.ingest(
                aggregation='NONE',
                vep_version=82,
                vep_cache_version=82,
                tasks=['variant_load'],
                db_name='eva_hsapiens_grch38'
            )
            # multiple vcf files with no aggregation => merge
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/load_PRJEB12345_merged.vcf.properties')
            )

    def test_ingest_variant_load_basic_aggregation(self):
        with patch('eva_submission.eload_ingestion.get_properties_from_xml_file', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True):
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            self.eload.ingest(
                aggregation='basic',
                vep_version=82,
                vep_cache_version=82,
                tasks=['variant_load'],
                db_name='eva_hsapiens_grch38'
            )
            # multiple vcf files with aggregation => no merge
            for filename in ('test1.vcf', 'test2.vcf'):
                assert os.path.exists(
                    os.path.join(self.resources_folder, f'projects/PRJEB12345/load_{filename}.properties')
                )
