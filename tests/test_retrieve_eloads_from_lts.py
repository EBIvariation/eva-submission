import os
import shutil
from unittest import TestCase

import yaml

from bin import retrieve_eload_from_lts


class TestRetrieveEloadFromLTS(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')
    test_lts_folder = os.path.join(resources_folder, 'lts')
    config_file = os.path.join(test_lts_folder, 'retrieval_config_new.yml')
    eloads_folder = os.path.join(test_lts_folder, 'submissions')
    projects_folder = os.path.join(test_lts_folder, 'projects')
    retrieval_output_folder = os.path.join(test_lts_folder, 'output')

    def setUp(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        self.setup_config_file()

    def tearDown(self) -> None:
        shutil.rmtree(self.retrieval_output_folder)

    def setup_config_file(self):
        config_data = {
            'retrieve': {
                'eloads_archive_dir': self.eloads_folder,
                'projects_archive_dir': self.projects_folder,
                'eloads': {
                    'eloads_output_path': self.retrieval_output_folder,
                    'projects_output_path': self.retrieval_output_folder,
                    'eload_archives': [
                        {
                            'name': 'ELOAD_919.tar',
                            'retrieve_dirs': [
                                'ELOAD_919/10_submitted',
                                'ELOAD_919/18_brokering'
                            ],
                            'retrieve_files': [
                                'ELOAD_919/.ELOAD_919_config.yml.gz',
                                'ELOAD_919/vcf_files_mapping.csv.gz'
                            ],
                            'retrieve_associated_project': True,
                            'update_noah_paths': True
                        },
                        {
                            'name': 'ELOAD_920.tar',
                            'retrieve_associated_project': True,
                            'update_noah_paths': True
                        }
                    ]
                },
                'projects': {
                    'projects_output_path': self.retrieval_output_folder,
                    'project_archives': [
                        {
                            'name': 'PRJEB9374.tar'
                        }
                    ]
                }
            }
        }

        with open(self.config_file, 'w') as f:
            yaml.safe_dump(config_data, f, default_flow_style=False)

    def assert_files_are_uncompressed(self, dir_path):
        for root, directories, files in os.walk(dir_path):
            for file in files:
                if file[-7:] == '.vcf.gz':
                    continue
                else:
                    assert file[-3:] != '.gz'

    def assert_paths_are_updated(self, eload_dir, eload):
        eload_config = os.path.join(eload_dir, eload, f'.{eload}_config.yml')
        self.assertTrue(os.path.exists(eload_config))
        with open(eload_config, 'r') as f:
            for line in f:
                if '/nfs/production3' in line:
                    return False
            return True

    def test_eloads_projects_retrieval(self):
        retrieve_eload_from_lts.retrieve_eloads_and_projects(self.config_file)

        retrieved_output_dirs = ['ELOAD_919', 'ELOAD_920', 'PRJEB9374']
        for retrieved_dir in retrieved_output_dirs:
            dir_path = os.path.join(self.retrieval_output_folder, retrieved_dir)
            self.assertTrue(os.path.exists(dir_path))
            self.assert_files_are_uncompressed(dir_path)

            if retrieved_dir[:5] == 'ELOAD':
                self.assertTrue(self.assert_paths_are_updated(self.retrieval_output_folder, retrieved_dir))
