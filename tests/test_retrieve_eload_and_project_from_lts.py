import os
import shutil
from unittest import TestCase

from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import load_config


class TestRetrieveEloadFromLTS(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_dir = os.path.join(os.path.dirname(__file__), 'resources')
    eloads_lts_dir = os.path.join(resources_dir, 'lts', 'submissions')
    projects_lts_dir = os.path.join(resources_dir, 'lts', 'projects')
    retrieval_output_dir = os.path.join(resources_dir, 'lts', 'output')
    codon_eloads_dir = os.path.join(resources_dir, 'lts', 'output', 'codon', 'eloads')
    codon_projects_dir = os.path.join(resources_dir, 'lts', 'output', 'codon', 'projects')
    config_file = os.path.join(resources_dir, 'lts', 'submission_config.yml')

    def setUp(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        with open(self.config_file, 'w') as file:
            file.write(f"eloads_dir: '{self.codon_eloads_dir}'\n")
            file.write(f"projects_dir: '{self.codon_projects_dir}'\n")
            file.write(f"eloads_lts_dir: '{self.eloads_lts_dir}'\n")
            file.write(f"projects_lts_dir: '{self.projects_lts_dir}'")

        load_config(self.config_file)

    def tearDown(self) -> None:
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        shutil.rmtree(self.retrieval_output_dir)

    def assert_files_are_uncompressed(self, dir_path):
        for root, directories, files in os.walk(dir_path):
            for file in files:
                if file[-7:] == '.vcf.gz':
                    continue
                else:
                    assert file[-3:] != '.gz'

    def check_paths_are_updated(self, eload_dir, eload):
        eload_config = os.path.join(eload_dir, f'.ELOAD_{eload}_config.yml')
        self.assertTrue(os.path.exists(eload_config))

        with open(eload_config, 'r') as f:
            for line in f:
                if '/nfs/production3' in line:
                    return False
            return True


    def test_eload_retrieval_full_with_associated_project_full(self):
        eload_retrieval = ELOADRetrieval()
        eload_retrieval.retrieve_eloads_and_projects(920, True, True, '', None, '', None, None,
                                                     self.retrieval_output_dir, self.retrieval_output_dir)

        eload_dir_path = os.path.join(self.retrieval_output_dir, 'ELOAD_920')
        self.assertTrue(os.path.exists(eload_dir_path))
        expected_eload_download_files_dirs = set(['.ELOAD_920_config.yml', '10_submitted', '13_validation', '18_brokering',
                                'ELOAD_920_submission.log', 'broker.err', 'broker.txt', 'brokering_config_file.yaml',
                                'out.err', 'out.txt', 'validation_confg_file.yaml', 'vcf_files_mapping.csv',
                                            'vcf_files_to_ingest.csv'])
        self.assertEqual(expected_eload_download_files_dirs, set(os.listdir(eload_dir_path)))
        self.assert_files_are_uncompressed(eload_dir_path)
        self.assertTrue(self.check_paths_are_updated(eload_dir_path, 920))

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB51612')
        self.assertTrue(os.path.exists(project_dir_path))
        expected_project_download_files_dirs = set(['10_submitted', '20_scratch', '21_validation', '30_eva_valid',
                                                    '40_transformed', '50_stats', '51_annotation','52_accessions',
                                                    '60_eva_public', '70_external_submissions', '80_deprecated',
                                                    'analysis', 'data'])
        self.assertEqual(expected_project_download_files_dirs, set(os.listdir(project_dir_path)))

        self.assert_files_are_uncompressed(project_dir_path)

    def test_eload_retrieval_partial_with_associated_project_partial(self):
        eload_retrieval = ELOADRetrieval()
        eload_retrieval.retrieve_eloads_and_projects(920, True, True, ['ELOAD_920/10_submitted', 'ELOAD_920/18_brokering',
                                                                   'ELOAD_920/.ELOAD_920_config.yml.gz'], None,
                                                     ['PRJEB51612/30_eva_valid', 'PRJEB51612/40_transformed'], None,
                                                     None, self.retrieval_output_dir, self.retrieval_output_dir)

        eload_dir_path = os.path.join(self.retrieval_output_dir, 'ELOAD_920')
        self.assertTrue(os.path.exists(eload_dir_path))
        expected_eload_download_files_dirs = set(['10_submitted', '.ELOAD_920_config.yml', '18_brokering'])
        self.assertEqual(expected_eload_download_files_dirs, set(os.listdir(eload_dir_path)))
        self.assert_files_are_uncompressed(eload_dir_path)
        self.assertTrue(self.check_paths_are_updated(eload_dir_path, 920))

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB51612')
        self.assertTrue(os.path.exists(project_dir_path))
        expected_project_download_files_dirs = set(['40_transformed', '30_eva_valid'])
        self.assertEqual(expected_project_download_files_dirs, set(os.listdir(project_dir_path)))
        self.assert_files_are_uncompressed(project_dir_path)

    def test_eloads_retrieval_no_associated_project(self):
        eload_retrieval = ELOADRetrieval()
        eload_retrieval.retrieve_eloads_and_projects(920, False, True, '', '', '', None, None,
                                                self.retrieval_output_dir, '')

        eload_dir_path = os.path.join(self.retrieval_output_dir, 'ELOAD_920')
        self.assertTrue(os.path.exists(eload_dir_path))
        self.assertTrue(os.path.exists(eload_dir_path))
        expected_eload_download_files_dirs = set(['.ELOAD_920_config.yml', '10_submitted', '13_validation', '18_brokering',
             'ELOAD_920_submission.log', 'broker.err', 'broker.txt', 'brokering_config_file.yaml',
             'out.err', 'out.txt', 'validation_confg_file.yaml', 'vcf_files_mapping.csv',
             'vcf_files_to_ingest.csv'])
        self.assertEqual(expected_eload_download_files_dirs, set(os.listdir(eload_dir_path)))

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB51612')
        self.assertFalse(os.path.exists(project_dir_path))

    def test_project_retrieval_only(self):
        eload_retrieval = ELOADRetrieval()
        eload_retrieval.retrieve_eloads_and_projects(None, False, True, None, 'PRJEB51612', None, None, None,
                                                     None, self.retrieval_output_dir)

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB51612')
        self.assertTrue(os.path.exists(project_dir_path))
        expected_project_download_files_dirs = set(['10_submitted', '20_scratch', '21_validation', '30_eva_valid',
                                                    '40_transformed', '50_stats', '51_annotation', '52_accessions',
                                                    '60_eva_public', '70_external_submissions', '80_deprecated',
                                                    'analysis', 'data'])
        self.assertEqual(expected_project_download_files_dirs, set(os.listdir(project_dir_path)))
        self.assert_files_are_uncompressed(project_dir_path)
