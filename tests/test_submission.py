import glob
import json
import os
import tempfile
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

import yaml

from eva_submission import ROOT_DIR, __version__, submission
from eva_submission.biosample_submission.sample_record import SampleRecord
from eva_submission.config_migration import upgrade_samples_to_sample_records
from eva_submission.submission import Submission
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.submission_config import SubmissionConfig, load_config


class TestEload(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.eload = Submission('submission_55')
        self.original_v0_config = SubmissionConfig(os.path.join(self.eload.submission_dir, 'original_v0_config.yml'))
        self.updated_v1_config = SubmissionConfig(os.path.join(self.eload.submission_dir, 'updated_v1_config.yml'))
        self.original_v1_14_config = SubmissionConfig(os.path.join(self.eload.submission_dir, 'original_v1_14_config.yml'))
        self.updated_v1_15_config = SubmissionConfig(os.path.join(self.eload.submission_dir, 'updated_v1_15_config.yml'))

        # Setup the config
        self.eload.submission_cfg.content = deepcopy(self.original_v0_config.content)
        self.original_updated_cfg = deepcopy(self.updated_v1_config.content)
        self.updated_v1_config.set('version', value=__version__)
        self.updated_v1_15_config.set('version', value=__version__)
        # Get the log file name
        self.logfile_name = os.path.join(self.eload.submission_dir, str(self.eload.submission_id) + "_submission.log")
        # The sample records upgrade looks in EVAPRO first: these analyses are not there
        self.evapro_patcher = patch.object(EvaProjectLoader, 'get_samples_in_analysis', return_value=[])
        self.evapro_patcher.start()

    def tearDown(self):
        self.evapro_patcher.stop()
        self.updated_v1_config.content = self.original_updated_cfg
        # remove the config its backups and the log file
        for file_path in [self.eload.submission_cfg.config_file, self.logfile_name] + \
                glob.glob(f'{self.eload.submission_cfg.config_file}.*'):
            if os.path.exists(file_path):
                os.remove(file_path)
        submission.submission_logging_files.clear()

    def test_create_log_file(self):
        # Creating a second eload object to test whether the logging file handler
        # has been created twice
        eload2 = Submission(self.eload.submission_id)

        self.eload.info("Testing the creation of logging file")

        assert os.path.exists(self.logfile_name)

        with open(self.logfile_name, "r") as test_logfile:
            k = [i for i in test_logfile.readlines() if "Testing the creation of logging file" in i]

            # Checking if the log message is written only once in the log file
            assert len(k) == 1

    def test_context_manager(self):
        with Submission('submission_55') as eload:
            assert eload.submission_cfg.query('submission', 'assembly_accession') is None
            eload.submission_cfg.set('submission', 'assembly_accession', value='GCA_00009999.9')
            assert eload.submission_cfg.query('submission', 'assembly_accession') == 'GCA_00009999.9'

        # Config file gets written
        with open(eload.submission_cfg.config_file) as open_file:
            config_dict = yaml.safe_load(open_file)
            assert config_dict['submission']['assembly_accession'] == 'GCA_00009999.9'

    def test_upgrade_config(self):
        """Tests config upgrade for a post-brokering config."""
        self.eload.upgrade_to_new_version_if_needed('analysis alias')
        self.assertEqual(self.updated_v1_config.content, self.eload.submission_cfg.content)

    def test_upgrade_config_no_analysis_alias_passed(self):
        """If no analysis alias is passed, it should retrieve from metadata when possible."""
        with patch('eva_submission.config_migration.EvaXlsxReader') as mock_reader:
            mock_reader.return_value.analysis = [{'Analysis Alias': 'analysis alias'}]
            self.eload.upgrade_to_new_version_if_needed(analysis_alias=None)
            self.assertEqual(self.updated_v1_config.content, self.eload.submission_cfg.content)

    def test_upgrade_config_already_updated(self):
        """An already up-to-date config shouldn't get modified."""
        original_content = deepcopy(self.updated_v1_config.content)
        self.eload.upgrade_to_new_version_if_needed('analysis alias')
        self.assertEqual(original_content, self.updated_v1_config.content)

    def test_upgrade_config_v1_15(self):
        """Tests config upgrade for a post-brokering config."""
        self.eload.submission_cfg.content = deepcopy(self.original_v1_14_config.content)
        self.eload.upgrade_to_new_version_if_needed()
        self.assertEqual(self.updated_v1_15_config.content, self.eload.submission_cfg.content)


class TestEloadSampleRecords(TestCase):

    def setUp(self):
        load_config(os.path.join(ROOT_DIR, 'tests', 'resources', 'submission_config.yml'))
        os.chdir(ROOT_DIR)
        self.eload = Submission('submission_55')
        self.eload.submission_cfg.content = {}

    def tearDown(self):
        for file_path in [self.eload.submission_cfg.config_file, f'{self.eload.submission_cfg.config_file}.1']:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_sample_records_per_analysis(self):
        self.eload.submission_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            # Same sampleInVCF in two genotype analyses
            {'accession': 'SAME000001', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': ['submission_55_GT2'], 'sampleInVCF': 'S1'},
            # AF samples with and without sampleInVCF or name
            {'accession': 'SAME000003', 'analysisAlias': ['AF1'], 'sampleInVCF': 'P1', 'name': 'bsd3'},
            {'accession': 'SAME000004', 'analysisAlias': ['AF1'], 'name': 'bsd4'},
            {'accession': 'SAME000005', 'analysisAlias': ['AF1']},
            {'accession': 'SAME000004', 'analysisAlias': ['AF1'], 'name': 'other_name'},
        ])
        assert self.eload.sample_name_to_accession_for_analysis('GT1') == {'S1': 'SAME000001'}
        assert self.eload.sample_name_to_accession_for_analysis('submission_55_GT2') == {'S1': 'SAME000002'}
        assert self.eload.sample_name_to_accession_for_analysis('GT2') == {'S1': 'SAME000002'}
        assert self.eload.sample_accession_to_name_for_analysis('AF1') == {
            'SAME000003': 'P1', 'SAME000004': 'bsd4', 'SAME000005': 'SAME000005'
        }

    def test_sample_records_for_analysis(self):
        self.eload.submission_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            {'accession': 'SAME000001', 'analysisAlias': ['GT1', 'AF1'], 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': ['submission_55_AF1'], 'name': 'bsd2'},
            {'accession': 'SAME000003', 'analysisAlias': ['GT1']},
        ])
        # Aliases match whether or not they are prefixed with the ELOAD
        assert self.eload.sample_records_for_analysis('AF1') == [
            SampleRecord('SAME000001', analysis_aliases=['GT1', 'AF1'], sample_in_vcf='S1'),
            SampleRecord('SAME000002', analysis_aliases=['submission_55_AF1'], name='bsd2'),
        ]
        assert [r.accession for r in self.eload.sample_records_for_analysis('submission_55_GT1')] == [
            'SAME000001', 'SAME000003'
        ]

    def test_sample_name_with_multiple_accessions_in_analysis(self):
        self.eload.submission_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            {'accession': 'SAME000001', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
            {'accession': 'SAME000002', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1'},
        ])
        with self.assertRaises(ValueError):
            self.eload.sample_name_to_accession_for_analysis('GT1')

    def _set_legacy_samples(self, sample_name_to_accession):
        self.eload.submission_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
        self.eload.submission_cfg.set('brokering', 'analyses', value={'submission_55_GT1': {}, 'submission_55_AF1': {}})
        self.eload.submission_cfg.set('brokering', 'ena', 'ANALYSIS',
                                      value={'submission_55_GT1': 'ERZ000001', 'submission_55_AF1': 'ERZ000002'})

    def _write_metadata_json(self, samples, tmp_dir):
        metadata_json = os.path.join(tmp_dir, 'metadata.json')
        with open(metadata_json, 'w') as open_file:
            json.dump({'sample': samples}, open_file)
        self.eload.submission_cfg.set('validation', 'valid', 'metadata_json', value=metadata_json)

    def test_upgrade_sample_records_from_evapro(self):
        self._set_legacy_samples({'S1': 'SAME000001', 'P1': 'SAME000002'})
        samples_in_evapro = {
            'ERZ000001': [('SAME000001', 'S1')],
            # The allele frequency sample without name was loaded with its accession as name
            'ERZ000002': [('SAME000001', 'S1'), ('SAME000002', 'SAME000002')],
        }
        upgrade_samples_to_sample_records(self.eload.submission_cfg, samples_in_evapro.get)
        assert self.eload.sample_records() == [
            SampleRecord('SAME000001', analysis_aliases=['submission_55_GT1', 'submission_55_AF1'], sample_in_vcf='S1'),
            SampleRecord('SAME000002', analysis_aliases=['submission_55_AF1']),
        ]
        assert self.eload.submission_cfg.query('brokering', 'Biosamples', 'Samples') is None

    def test_upgrade_sample_records_from_metadata(self):
        self._set_legacy_samples({'S1': 'SAME000001'})
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._write_metadata_json([
                {'analysisAlias': ['GT1'], 'sampleInVCF': 'S1', 'bioSampleObject': {'name': 'LH1'}},
                {'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000003'},
            ], tmp_dir)
            # The second analysis is not in EVAPRO
            upgrade_samples_to_sample_records(self.eload.submission_cfg, {'ERZ000001': [('SAME000001', 'S1')]}.get)
        assert self.eload.sample_records() == [
            SampleRecord('SAME000001', analysis_aliases=['GT1'], sample_in_vcf='S1'),
            SampleRecord('SAME000003', analysis_aliases=['AF1']),
        ]

    def test_upgrade_sample_records_from_sample_map(self):
        self._set_legacy_samples({'S1': 'SAME000001', 'S2': 'SAME000002'})
        with tempfile.TemporaryDirectory() as tmp_dir:
            # S3 has no accession: the metadata cannot be used
            self._write_metadata_json([{'analysisAlias': ['GT1'], 'sampleInVCF': 'S3'}], tmp_dir)
            with self.assertLogs('eva_submission.config_migration', level='WARNING'):
                upgrade_samples_to_sample_records(self.eload.submission_cfg, samples_in_evapro_analysis=None)
        # Each sample is assigned to all the analyses, as the legacy dict was
        assert self.eload.sample_records() == [
            SampleRecord('SAME000001', analysis_aliases=['submission_55_GT1', 'submission_55_AF1'], sample_in_vcf='S1'),
            SampleRecord('SAME000002', analysis_aliases=['submission_55_GT1', 'submission_55_AF1'], sample_in_vcf='S2'),
        ]
        assert self.eload.sample_name_to_accession_for_analysis('GT1') == {'S1': 'SAME000001', 'S2': 'SAME000002'}
        assert self.eload.sample_accession_to_name_for_analysis('AF1') == {'SAME000001': 'S1', 'SAME000002': 'S2'}

    def test_upgrade_sample_records_if_needed(self):
        self._set_legacy_samples({'S1': 'SAME000001'})
        with patch.object(EvaProjectLoader, 'get_samples_in_analysis', side_effect=Exception('No EVAPRO')):
            self.eload.upgrade_sample_records_if_needed()
        assert self.eload.sample_records() == [
            SampleRecord('SAME000001', analysis_aliases=['submission_55_GT1', 'submission_55_AF1'], sample_in_vcf='S1')
        ]
        # Configs that already have sample records are not changed
        with patch.object(EvaProjectLoader, 'get_samples_in_analysis') as m_evapro:
            self.eload.upgrade_sample_records_if_needed()
        m_evapro.assert_not_called()
