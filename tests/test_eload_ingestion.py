import glob
import os
import shutil
import subprocess
from unittest import TestCase, mock
from unittest.mock import patch

from ebi_eva_common_pyutils.config import cfg
import requests

from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config


class TestEloadIngestion(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        cfg.content['executable'] = {
            'load_from_ena': 'path_to_load_script'
        }
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        # Set up a working eload config
        self.eload = EloadIngestion(3)
        self.eload.eload_cfg.set('submission', 'assembly_accession', value='GCA_002863925.1')
        self.eload.eload_cfg.set('brokering', 'ena', 'PROJECT', value='PRJEB12345')

    def tearDown(self):
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_3'))
        for eload in eloads:
            shutil.rmtree(eload)

    def _mock_mongodb_client(self):
        m_db = mock.Mock()
        m_db.list_database_names = mock.Mock(return_value=[
            'eva_ecaballus_30',
            'eva_hsapiens_grch38'
        ])
        return m_db

    def test_get_db_name(self):
        with patch('eva_submission.eload_ingestion.get_pg_metadata_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_query_results:
            m_get_query_results.return_value = [('ecaballus', '30')]
            self.assertEqual('eva_ecaballus_30', self.eload.get_db_name())

    def test_get_db_name_missing_evapro(self):
        with patch('eva_submission.eload_ingestion.get_pg_metadata_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_query_results:
            m_get_query_results.return_value = []
            with self.assertRaises(ValueError):
                self.eload.get_db_name()

    def test_get_db_name_multiple_evapro(self):
        with patch('eva_submission.eload_ingestion.get_pg_metadata_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_query_results:
            m_get_query_results.return_value = [('ecaballus', '30'), ('ecaballus', '20')]
            with self.assertRaises(ValueError):
                self.eload.get_db_name()

    def test_check_variant_db(self):
        with patch('eva_submission.eload_ingestion.get_pg_metadata_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.psycopg2.connect', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.get_mongo_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_results.return_value = [('ecaballus', '30')]
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()

            self.eload.check_variant_db()
            self.assertEqual(
                'eva_ecaballus_30',
                self.eload.eload_cfg.query('ingestion', 'database', 'db_name')
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'exists')

    def test_check_variant_db_name_provided(self):
        with patch('eva_submission.eload_ingestion.get_mongo_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.check_variant_db(db_name='eva_hsapiens_grch38')
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'db_name'),
                'eva_hsapiens_grch38'
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'exists')

    def test_check_variant_db_missing(self):
        with patch('eva_submission.eload_ingestion.get_mongo_uri_for_eva_profile', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
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
        self.eload.eload_cfg.set('brokering', value={})
        with self.assertRaises(ValueError):
            self.eload.load_from_ena()

    def test_load_from_ena_script_fails(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()
