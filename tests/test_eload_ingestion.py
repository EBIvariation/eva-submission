import glob
import os
import shutil
import subprocess
from copy import deepcopy
from unittest import TestCase, mock
from unittest.mock import patch, MagicMock

import yaml

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config


def default_db_results_for_update_metadata():
    # The update of metadata at the end of execution
    browsable_files = [(1, 'ERA', 'filename_1', 'PRJ', 123), (2, 'ERA', 'filename_1', 'PRJ', 123)]
    return [
        browsable_files,        # insert_browsable_files files_query
        browsable_files,        # insert_browsable_files find_browsable_files_query
        [(1, 'GCA_999')],       # update_loaded_assembly_in_browsable_files
        [(1, 'filename_1'), (2, 'filename_2')]  # update_files_with_ftp_path
    ]


def default_db_results_for_metadata_load():
    return [
        [(391,)]  # Check the assembly_set_id in update_assembly_set_in_analysis
    ]


def default_db_results_for_target_assembly():
    return [
        [('GCA_999')]
    ]


def default_db_results_for_accession():
    return [
        [('Test Study Name')]  # get_study_name
    ]


def default_db_results_for_clustering():
    return [
        [('GCA_123',)]  # current supported assembly
    ]


def default_db_results_for_accession_and_load():
    return [
        [('Test Study Name',)]  # get_study_name
    ]


def default_db_results_for_accession():
    return default_db_results_for_accession_and_load() + default_db_results_for_update_metadata()


def default_db_results_for_ingestion():
    return (
            default_db_results_for_metadata_load()
            + default_db_results_for_accession_and_load()
            + default_db_results_for_clustering()
    )


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
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
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
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
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
        with patch('eva_submission.eload_ingestion.check_project_exists_in_evapro'), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_script_fails(self):
        with patch('eva_submission.eload_ingestion.check_project_exists_in_evapro'), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_ingest_all_tasks(self):
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_submission.get_hold_date_from_ena') as m_get_hold_date, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_get_results.side_effect = default_db_results_for_ingestion()
            m_get_tax.return_value = ('name', '9090')
            self.eload.ingest()

    def test_ingest_metadata_load(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            self.eload.ingest(tasks=['metadata_load'])

    def test_load_from_ena_from_analysis(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as mockrun:
            analysis_accession = 'ERZ2499196'
            self.eload.load_from_ena_from_project_or_analysis(analysis_accession)
            command = ('perl /path/to/load_from_ena_script -p PRJEB12345 -c submitted -v 1 -l '
                       f'{self.eload._get_dir("scratch")} -e 33 -A -a ERZ2499196')
            mockrun.assert_called_once_with('Load metadata from ENA to EVAPRO', command)

    def test_ingest_accession(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_accession()
            self.eload.ingest(
                tasks=['accession']
            )
            assert os.path.exists(
                os.path.join(self.eload.eload_dir, 'accession_and_load_params.yaml')
            )

    def test_ingest_variant_load(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_accession()
            self.eload.ingest(tasks=['variant_load'])
            assert os.path.exists(
                os.path.join(self.eload.eload_dir, 'accession_and_load_params.yaml')
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
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_accession()
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            self.eload.ingest(tasks=['variant_load'])
            self.assert_vep_versions(100, 100, 'homo_sapiens')

    def test_ingest_variant_load_vep_versions_not_found(self):
        """
        If VEP cache version is not found but no exception is raised, we should proceed with variant load
        but skip annotation.
        """
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_accession_and_load()
            m_get_vep_versions.return_value = (None, None)
            m_get_species.return_value = 'homo_sapiens'
            self.eload.ingest(tasks=['variant_load'])
            self.assert_vep_versions('', '', '')

    def test_ingest_variant_load_vep_versions_error(self):
        """
        If getting VEP cache version raises an exception, we should stop the loading process altogether.
        """
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_accession_and_load()
            m_get_vep_versions.side_effect = ValueError()
            with self.assertRaises(ValueError):
                self.eload.ingest(tasks=['variant_load'])
            config_file = os.path.join(self.resources_folder, 'projects/PRJEB12345/accession_and_load_params.yaml')
            assert not os.path.exists(config_file)


    def test_ingest_clustering(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_results.side_effect = default_db_results_for_clustering()
            m_get_tax.return_value = ('name', '9796')
            self.eload.ingest(tasks=['optional_remap_and_cluster'])
            assert self.eload.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly') == 'GCA_123'
            assert m_run_command.call_count == 1

    def test_ingest_clustering_no_supported_assembly(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch.object(EloadIngestion, '_get_target_assembly') as m_target_assembly, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_target_assembly.return_value = None
            m_get_results.return_value = []
            self.eload.ingest(tasks=['optional_remap_and_cluster'])
            assert self.eload.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly') is None
            assert m_run_command.call_count == 0

    def test_ingest_clustering_supported_assembly_in_another_taxonomy(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch.object(EloadIngestion, "_get_supported_assembly_from_evapro", new=MagicMock()) as m_get_supported_asm, \
                patch.object(EloadIngestion, "_insert_new_supported_asm_from_ensembl", new=MagicMock()) as m_new_supported_asm, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_tax.return_value = ('name', 66666)
            m_get_supported_asm.side_effect = [None, 'gca_in_another_tax']
            m_new_supported_asm.return_value = None
            m_get_results.return_value = []
            self.eload.ingest(tasks=['optional_remap_and_cluster'])
            assert self.eload.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly') == 'gca_in_another_tax'
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch.object(EloadIngestion, "_get_supported_assembly_from_evapro", new=MagicMock()) as m_get_supported_asm, \
                patch.object(EloadIngestion, "_insert_new_supported_asm_from_ensembl", new=MagicMock()) as m_new_supported_asm, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_tax.return_value = ('name', 66666)
            m_new_supported_asm.side_effect = [None, 'gca_in_another_tax']
            m_get_supported_asm.return_value = None
            m_get_results.return_value = []
            self.eload.ingest(tasks=['optional_remap_and_cluster'])
            assert self.eload.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly') == 'gca_in_another_tax'

    def test_resume_when_step_fails(self):
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = (default_db_results_for_metadata_load()
                                         + default_db_results_for_accession_and_load()
                                         + default_db_results_for_ingestion())

            m_run_command.side_effect = [
                None,  # metadata load
                subprocess.CalledProcessError(1, 'nextflow accession'),  # first accession fails
                None,  # metadata load on resume
                None,  # accession on resume
                None,  # clustering
                None,  # variant load
            ]
            m_get_tax.return_value = ('name', '9090')

            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.ingest()
            for task in ['accession', 'variant_load']:
                nextflow_dir = self.eload.eload_cfg.query(self.eload.config_section, 'accession_and_load', 'nextflow_dir', task)
                assert os.path.exists(nextflow_dir)

            self.eload.ingest(resume=True)
            for task in ['accession', 'variant_load']:
                assert self.eload.eload_cfg.query(self.eload.config_section, 'accession_and_load', 'nextflow_dir', task) == '<complete>'
            assert not os.path.exists(nextflow_dir)

    def test_resume_completed_job(self):
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_ingestion.get_assembly_name_and_taxonomy_id') as m_get_tax, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = default_db_results_for_ingestion() + default_db_results_for_ingestion()
            m_get_tax.return_value = ('name', '9796')

            # Resuming with no existing job execution is fine
            self.eload.ingest(resume=True)
            num_db_calls = m_get_results.call_count
            assert m_run_command.call_count == 3

            # If we resume a successfully completed job, everything in the python will re-run (including db queries)
            # but the nextflow calls will not
            self.eload.ingest(resume=True)
            assert m_get_results.call_count == 2 * num_db_calls
            assert m_run_command.call_count == 4  # 1 per task, plus 1 for metadata load

    def test_resume_with_tasks(self):
        with self._patch_metadata_handle(), \
                patch.object(EloadIngestion, '_update_metadata_post_ingestion') as m_post_load_metadata, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_ingestion.get_species_name_from_ncbi') as m_get_species, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                patch('eva_submission.eload_ingestion.insert_new_assembly_and_taxonomy') as insert_asm_tax, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100)
            m_get_species.return_value = 'homo_sapiens'
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_results.side_effect = (
                    default_db_results_for_accession_and_load()
                    + default_db_results_for_clustering()
                    + default_db_results_for_accession_and_load()
            )

            m_run_command.side_effect = [
                subprocess.CalledProcessError(1, 'nextflow accession'),  # first accession fails
                None,  # remapping run alone
                None,  # accession on resume
            ]
            accession_config_section = 'accession_and_load'
            remap_config_section = 'remap_and_cluster'

            # Accession fails...
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.ingest(tasks=['accession'], resume=True)
            accession_nextflow_dir = self.eload.eload_cfg.query(self.eload.config_section, accession_config_section,
                                                                'nextflow_dir', 'accession')
            assert os.path.exists(accession_nextflow_dir)

            # ...doesn't resume when we run just optional_remap_and_cluster (successfully)...
            self.eload.ingest(tasks=['optional_remap_and_cluster'], resume=True)
            new_remap_nextflow_dir = self.eload.eload_cfg.query(self.eload.config_section, remap_config_section, 'nextflow_dir', 'optional_remap_and_cluster')
            assert new_remap_nextflow_dir != accession_nextflow_dir
            assert os.path.exists(accession_nextflow_dir)
            assert not os.path.exists(new_remap_nextflow_dir)
            assert new_remap_nextflow_dir == self.eload.nextflow_complete_value

            # ...and does resume when we run accession again.
            self.eload.ingest(tasks=['accession'], resume=True)
            new_accession_nextflow_dir = self.eload.eload_cfg.query(self.eload.config_section, accession_config_section,
                                                                    'nextflow_dir', 'accession')
            assert new_accession_nextflow_dir == self.eload.nextflow_complete_value
            assert not os.path.exists(accession_nextflow_dir)

    def test_get_target_assembly_fallback_on_submitted_assembly(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query'), \
                patch.object(EloadIngestion, '_insert_new_supported_asm_from_ensembl') as m_ensembl_asm:
            m_ensembl_asm.return_value = None  # Pretend Ensembl supports nothing
            self.assertEqual(self.eload._get_target_assembly(), list(self.eload.assembly_accessions)[0])

    def _patch_pre_run_nextflow(self):
        return patch.object(EloadIngestion, 'create_nextflow_temp_output_directory', return_value='work_dir'), \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_ingestion.shutil.rmtree')

    def _post_run_nextflow_assert(self, m_run_command, workflow_name, work_dir, resume, task_performed, task_completed):
        for task in task_completed:
            assert self.eload.eload_cfg.query('ingestion', workflow_name, 'nextflow_dir', task) == '<complete>'
        nextflow_script = os.path.join(NEXTFLOW_DIR, f'{workflow_name}.nf')
        command = (f'export NXF_OPTS="-Xms1g -Xmx8g";  '
                   f'/path/to/nextflow {nextflow_script} -params-file {self.eload.project_dir}/workflow_params.yaml '
                   f'-work-dir {work_dir} ')
        command += '-resume ' if resume else ' '
        m_run_command.assert_called_once_with('Nextflow workflow process', command)
        with open(os.path.join(self.eload.project_dir, 'workflow_params.yaml')) as open_file:
            params = yaml.safe_load(open_file)
            assert sorted(params['tasks']) == sorted(task_performed)

    def test_run_nextflow(self):
        p_cr, p_cmd, p_rm = self._patch_pre_run_nextflow()
        workflow_name = 'workflow'
        param_values = {'key': 'value'}
        self.eload.project_dir = os.path.join(self.resources_folder, 'projects', 'PRJEB12345')
        os.makedirs(self.eload.project_dir, exist_ok=True)
        tasks = ['task1']
        with p_cr, p_cmd as m_run_command, p_rm:
            self.eload.run_nextflow(workflow_name, param_values, resume=False, tasks=tasks)
            self._post_run_nextflow_assert(m_run_command, workflow_name, work_dir='work_dir', resume=False,
                                           task_performed=tasks, task_completed=tasks)

    def test_run_nextflow_resume(self):
        p_cr, p_cmd, p_rm = self._patch_pre_run_nextflow()
        workflow_name = 'workflow'
        param_values = {'key': 'value'}
        self.eload.project_dir = os.path.join(self.resources_folder, 'projects', 'PRJEB12345')
        nextflow_dir = os.path.join(self.eload.project_dir, 'nextflow_dir')
        os.makedirs(nextflow_dir, exist_ok=True)
        tasks = ['task1', 'task2']

        with p_cr, p_cmd as m_run_command, p_rm:
            self.eload.eload_cfg.set('ingestion', workflow_name, 'nextflow_dir', 'task1', value='<complete>')
            self.eload.eload_cfg.set('ingestion', workflow_name, 'nextflow_dir', 'task2', value=nextflow_dir)
            self.eload.run_nextflow(workflow_name, param_values, resume=True, tasks=tasks)
            # Reuse the existing nextflow_dir
            self._post_run_nextflow_assert(m_run_command, workflow_name, work_dir=nextflow_dir, resume=True,
                                           task_performed=['task2'], task_completed=tasks)

    def test_run_nextflow_resume_incompatible(self):
        p_cr, p_cmd, p_rm = self._patch_pre_run_nextflow()
        workflow_name = 'workflow'
        param_values = {'key': 'value'}
        self.eload.project_dir = os.path.join(self.resources_folder, 'projects', 'PRJEB12345')
        os.makedirs(self.eload.project_dir, exist_ok=True)
        nextflow_dir1 = os.path.join(self.eload.project_dir, 'nextflow_dir1')
        nextflow_dir2 = os.path.join(self.eload.project_dir, 'nextflow_dir2')
        os.makedirs(nextflow_dir1, exist_ok=True)
        os.makedirs(nextflow_dir2, exist_ok=True)
        tasks = ['task1', 'task2']
        with p_cr, p_cmd as m_run_command, p_rm:
            self.eload.eload_cfg.set('ingestion', workflow_name, 'nextflow_dir', 'task1', value=nextflow_dir1)
            self.eload.eload_cfg.set('ingestion', workflow_name, 'nextflow_dir', 'task2', value=nextflow_dir2)
            self.eload.run_nextflow(workflow_name, param_values, resume=True, tasks=tasks)
            # Cannot reuse the existing nextflow_dir1 or nextflow_dir2 because they are not compatible
            self._post_run_nextflow_assert(m_run_command, workflow_name, work_dir='work_dir', resume=True,
                                           task_performed=tasks, task_completed=tasks)

