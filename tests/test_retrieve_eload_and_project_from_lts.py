import os
import shutil
from unittest import TestCase

from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import load_config


class TestRetrieveEloadFromLTS(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_dir = os.path.join(os.path.dirname(__file__), 'resources')
    lts_eloads_dir = os.path.join(resources_dir, 'lts', 'submissions')
    lts_projects_dir = os.path.join(resources_dir, 'lts', 'projects')
    retrieval_output_dir = os.path.join(resources_dir, 'lts', 'output')
    codon_eloads_dir = os.path.join(resources_dir, 'lts', 'output', 'codon', 'eloads')
    codon_projects_dir = os.path.join(resources_dir, 'lts', 'output', 'codon', 'projects')
    config_file = os.path.join(resources_dir, 'lts', 'submission_config.yml')

    def setUp(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        with open(self.config_file, 'w') as file:
            file.write(f"eloads_dir: '{self.codon_eloads_dir}'\n")
            file.write(f"projects_dir: '{self.codon_projects_dir}'")

        load_config(self.config_file)

    def tearDown(self) -> None:
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

    def test_eloads_and_project_full_retrieval(self):
        self.eload = ELOADRetrieval(920)

        self.eload.retrieve_eloads_and_projects(920, True, True, '', 'PRJEB9374', '', self.lts_eloads_dir,
                                                self.lts_projects_dir, self.retrieval_output_dir,
                                                self.retrieval_output_dir)

        eload_dir_path = os.path.join(self.retrieval_output_dir, 'ELOAD_920')
        self.assertTrue(os.path.exists(eload_dir_path))
        self.assert_files_are_uncompressed(eload_dir_path)
        self.assertTrue(self.check_paths_are_updated(eload_dir_path, 920))

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB51612')
        self.assertTrue(os.path.exists(project_dir_path))
        self.assert_files_are_uncompressed(project_dir_path)

        project_dir_path = os.path.join(self.retrieval_output_dir, 'PRJEB9374')
        self.assertTrue(os.path.exists(project_dir_path))
        self.assert_files_are_uncompressed(project_dir_path)

    def test_eloads_partial_retrieval(self):
        self.eload = ELOADRetrieval(919)

        self.eload.retrieve_eloads_and_projects(919, False, True, ['ELOAD_919/10_submitted', 'ELOAD_919/18_brokering',
                                                                   'ELOAD_919/ELOAD_919_submission.log.gz'], None, None,
                                                self.lts_eloads_dir, None, self.retrieval_output_dir, None)
        eload_dir_path = os.path.join(self.retrieval_output_dir, 'ELOAD_919')
        self.assertTrue(os.path.exists(eload_dir_path))

        # assert only the given dirs and files are retrieved
        self.assertTrue(os.path.exists(os.path.join(eload_dir_path, '10_submitted')))
        self.assertTrue(os.path.exists(os.path.join(eload_dir_path, '18_brokering')))
        self.assertTrue(os.path.exists(os.path.join(eload_dir_path, 'ELOAD_919_submission.log')))
        # assert 13_validation is not retrieved
        self.assertFalse(os.path.exists(os.path.join(eload_dir_path, '13_validation')))

        self.assert_files_are_uncompressed(eload_dir_path)
