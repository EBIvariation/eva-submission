import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

import yaml

from eva_submission import ROOT_DIR, __version__, eload_submission
from eva_submission.eload_submission import Eload
from eva_submission.submission_config import EloadConfig, load_config


class TestEload(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.eload = Eload(55)
        self.original_v0_config = EloadConfig(os.path.join(self.eload.eload_dir, 'original_v0_config.yml'))
        self.updated_v1_config = EloadConfig(os.path.join(self.eload.eload_dir, 'updated_v1_config.yml'))
        self.original_v1_14_config = EloadConfig(os.path.join(self.eload.eload_dir, 'original_v1_14_config.yml'))
        self.updated_v1_15_config = EloadConfig(os.path.join(self.eload.eload_dir, 'updated_v1_15_config.yml'))

        # Setup the config
        self.eload.eload_cfg.content = deepcopy(self.original_v0_config.content)
        self.original_updated_cfg = deepcopy(self.updated_v1_config.content)
        self.updated_v1_config.set('version', value=__version__)
        self.updated_v1_15_config.set('version', value=__version__)
        # Get the log file name
        self.logfile_name = os.path.join(self.eload.eload_dir, str(self.eload.eload) + "_submission.log")

    def tearDown(self):
        self.updated_v1_config.content = self.original_updated_cfg
        # remove the config its backup and the log file
        for file_path in [self.eload.eload_cfg.config_file, f'{self.eload.eload_cfg.config_file}.1', self.logfile_name]:
            if os.path.exists(file_path):
                os.remove(file_path)
        eload_submission.eload_logging_files.clear()

    def test_create_log_file(self):
        # Creating a second eload object to test whether the logging file handler
        # has been created twice
        eload2 = Eload(self.eload.eload_num)

        self.eload.info("Testing the creation of logging file")

        assert os.path.exists(self.logfile_name)

        with open(self.logfile_name, "r") as test_logfile:
            k = [i for i in test_logfile.readlines() if "Testing the creation of logging file" in i]

            # Checking if the log message is written only once in the log file
            assert len(k) == 1

    def test_context_manager(self):
        with Eload(55) as eload:
            assert eload.eload_cfg.query('submission', 'assembly_accession') is None
            eload.eload_cfg.set('submission', 'assembly_accession', value='GCA_00009999.9')
            assert eload.eload_cfg.query('submission', 'assembly_accession') == 'GCA_00009999.9'

        # Config file gets written
        with open(eload.eload_cfg.config_file) as open_file:
            config_dict = yaml.safe_load(open_file)
            assert config_dict['submission']['assembly_accession'] == 'GCA_00009999.9'

    def test_upgrade_config(self):
        """Tests config upgrade for a post-brokering config."""
        self.eload.upgrade_to_new_version_if_needed('analysis alias')
        self.assertEqual(self.updated_v1_config.content, self.eload.eload_cfg.content)

    def test_upgrade_config_no_analysis_alias_passed(self):
        """If no analysis alias is passed, it should retrieve from metadata when possible."""
        with patch('eva_submission.config_migration.EvaXlsxReader') as mock_reader:
            mock_reader.return_value.analysis = [{'Analysis Alias': 'analysis alias'}]
            self.eload.upgrade_to_new_version_if_needed(analysis_alias=None)
            self.assertEqual(self.updated_v1_config.content, self.eload.eload_cfg.content)

    def test_upgrade_config_already_updated(self):
        """An already up-to-date config shouldn't get modified."""
        original_content = deepcopy(self.updated_v1_config.content)
        self.eload.upgrade_to_new_version_if_needed('analysis alias')
        self.assertEqual(original_content, self.updated_v1_config.content)

    def test_upgrade_config_v1_15(self):
        """Tests config upgrade for a post-brokering config."""
        self.eload.eload_cfg.content = deepcopy(self.original_v1_14_config.content)
        self.eload.upgrade_to_new_version_if_needed()
        self.assertEqual(self.updated_v1_15_config.content, self.eload.eload_cfg.content)


class TestEloadSampleRecords(TestCase):

    def setUp(self):
        load_config(os.path.join(ROOT_DIR, 'tests', 'resources', 'submission_config.yml'))
        os.chdir(ROOT_DIR)
        self.eload = Eload(55)
        self.eload.eload_cfg.content = {}

    def tearDown(self):
        for file_path in [self.eload.eload_cfg.config_file, f'{self.eload.eload_cfg.config_file}.1']:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_sample_records_per_analysis(self):
        self.eload.eload_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            # Same sampleInVCF in two genotype analyses
            {'accession': 'SAME000001', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': ['ELOAD_55_GT2'], 'sampleInVCF': 'S1'},
            # AF samples with and without sampleInVCF or name
            {'accession': 'SAME000003', 'analysisAlias': ['AF1'], 'sampleInVCF': 'P1', 'name': 'bsd3'},
            {'accession': 'SAME000004', 'analysisAlias': ['AF1'], 'name': 'bsd4'},
            {'accession': 'SAME000005', 'analysisAlias': ['AF1']},
            {'accession': 'SAME000004', 'analysisAlias': ['AF1'], 'name': 'other_name'},
        ])
        assert self.eload.sample_name_to_accession_for_analysis('GT1') == {'S1': 'SAME000001'}
        assert self.eload.sample_name_to_accession_for_analysis('ELOAD_55_GT2') == {'S1': 'SAME000002'}
        assert self.eload.sample_name_to_accession_for_analysis('GT2') == {'S1': 'SAME000002'}
        assert self.eload.sample_accession_to_name_for_analysis('AF1') == {
            'SAME000003': 'P1', 'SAME000004': 'bsd4', 'SAME000005': 'SAME000005'
        }

    def test_sample_name_with_multiple_accessions_in_analysis(self):
        self.eload.eload_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            {'accession': 'SAME000001', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
        ])
        with self.assertRaises(ValueError):
            self.eload.sample_name_to_accession_for_analysis('GT1')

    def test_legacy_samples(self):
        self.eload.eload_cfg.set('brokering', 'Biosamples', 'Samples', value={'S1': 'SAME000001', 'S2': 'SAME000002'})
        assert self.eload.sample_records() == [
            {'accession': 'SAME000001', 'analysisAlias': None, 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': None, 'sampleInVCF': 'S2'},
        ]
        # Legacy records belong to every analysis
        assert self.eload.sample_name_to_accession_for_analysis('GT1') == {'S1': 'SAME000001', 'S2': 'SAME000002'}
        assert self.eload.sample_accession_to_name_for_analysis('AF1') == {'SAME000001': 'S1', 'SAME000002': 'S2'}
        assert self.eload.sample_name_to_accession() == {'S1': 'SAME000001', 'S2': 'SAME000002'}
