import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from eva_submission import ROOT_DIR
from eva_submission.biosample_submission.sample_record import SampleRecord
from eva_submission.submission import Submission
from eva_submission.submission_brokering import SubmissionBrokering
from eva_submission.submission_config import SubmissionConfig, load_config
from eva_submission.submission_preparation import SubmissionPreparation
from eva_submission.submission_validation import SubmissionValidation

DB_STATE = {
    'projectAccession': 'PRJEB0001',
    'releaseDate': '2030-01-01',
    'projectTaxonomies': '9606',
    'analyses': [
        {'analysisAlias': 'sub_state_a1', 'analysisAccession': 'ERZ0000001', 'aggregationType': 'basic',
         'samples': [{'sampleName': 'S1', 'sampleAccession': 'SAME1'}, {'sampleName': 'S2', 'sampleAccession': 'SAME2'}]},
        {'analysisAlias': 'sub_state_a2', 'analysisAccession': 'ERZ0000002', 'aggregationType': 'none'},
    ]
}


class SubmissionStateTestCase(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    submission_id = 'sub_state'
    submission_class = Submission

    def setUp(self):
        load_config(os.path.join(self.resources_folder, 'submission_config.yml'))
        os.chdir(ROOT_DIR)
        self.sub = self.submission_class(self.submission_id)
        self.sub.submission_cfg.content = {}

    def tearDown(self):
        SubmissionConfig.content = {}
        for path in [self.sub.submission_dir, self.sub.nobackup_submission_dir]:
            shutil.rmtree(path, ignore_errors=True)

    def with_db_state(self, state):
        return patch('eva_submission.submission.get_tracking_details', return_value=state)


class TestSubmissionState(SubmissionStateTestCase):

    def test_db_state_is_retrieved_once_and_refreshed(self):
        with self.with_db_state(DB_STATE) as m_get:
            assert self.sub.db_state == DB_STATE
            assert self.sub.db_state == DB_STATE
            m_get.assert_called_once_with(self.submission_id)
            self.sub.refresh_db_state()
            assert self.sub.db_state == DB_STATE
            assert m_get.call_count == 2

    def test_db_state_empty_when_unavailable(self):
        with self.with_db_state(None):
            assert self.sub.db_state == {}

    def test_store_state_writes_to_ws_and_refreshes(self):
        with patch('eva_submission.submission.update_tracking_details') as m_update, self.with_db_state({}):
            assert self.sub.db_state == {}
            self.sub.store_state(project_accession='PRJEB1', project_title='title')
            m_update.assert_called_once_with(self.submission_id, project_accession='PRJEB1', project_title='title')
            assert 'db_state' not in self.sub.__dict__

    def test_get_state_prefers_db(self):
        self.sub.submission_cfg.set('brokering', 'ena', 'PROJECT', value='PRJEB_CONFIG')
        with self.with_db_state(DB_STATE):
            assert self.sub.get_state('projectAccession', 'brokering', 'ena', 'PROJECT') == 'PRJEB0001'

    def test_get_state_falls_back_on_config(self):
        self.sub.submission_cfg.set('brokering', 'ena', 'PROJECT', value='PRJEB_CONFIG')
        with self.with_db_state(None):
            assert self.sub.get_state('projectAccession', 'brokering', 'ena', 'PROJECT') == 'PRJEB_CONFIG'
        with self.with_db_state({'analyses': []}):  # DB reachable but nothing stored for that field
            assert self.sub.get_state('projectAccession', 'brokering', 'ena', 'PROJECT') == 'PRJEB_CONFIG'

    def test_get_state_default(self):
        with self.with_db_state(None):
            assert self.sub.get_state('releaseDate', 'brokering', 'ena', 'hold_date', default='nope') == 'nope'

    def test_get_taxonomy_id(self):
        self.sub.submission_cfg.set('submission', 'taxonomy_id', value=1234)
        with self.with_db_state({'projectTaxonomies': '9606'}):
            assert self.sub.get_taxonomy_id() == 9606
        self.sub.refresh_db_state()
        with self.with_db_state({'projectTaxonomies': [9606]}):
            assert self.sub.get_taxonomy_id() == 9606
        self.sub.refresh_db_state()
        # Ambiguous or missing in the DB: use the config
        with self.with_db_state({'projectTaxonomies': '9606,10090'}):
            assert self.sub.get_taxonomy_id() == 1234
        self.sub.refresh_db_state()
        with self.with_db_state(None):
            assert self.sub.get_taxonomy_id() == 1234

    def test_get_analysis_accessions_and_aggregations_db_over_config(self):
        self.sub.submission_cfg.set('brokering', 'ena', 'ANALYSIS',
                                    value={'sub_state_a1': 'ERZ_OLD', 'sub_state_a3': 'ERZ0000003'})
        self.sub.submission_cfg.set('validation', 'aggregation_check', 'analyses',
                                    value={'sub_state_a1': 'none', 'sub_state_a3': 'basic'})
        with self.with_db_state(DB_STATE):
            assert self.sub.get_analysis_accessions() == {
                'sub_state_a1': 'ERZ0000001', 'sub_state_a2': 'ERZ0000002', 'sub_state_a3': 'ERZ0000003'}
            assert self.sub.get_analysis_aggregations() == {
                'sub_state_a1': 'basic', 'sub_state_a2': 'none', 'sub_state_a3': 'basic'}
            assert self.sub.get_analysis_aggregation('sub_state_a3') == 'basic'
            assert self.sub.get_analysis_aggregation('unknown') is None

    def test_get_analysis_accessions_and_aggregations_config_only(self):
        self.sub.submission_cfg.set('brokering', 'ena', 'ANALYSIS', value={'sub_state_a1': 'ERZ0000001'})
        self.sub.submission_cfg.set('validation', 'aggregation_check', 'analyses', value={'sub_state_a1': 'basic'})
        with self.with_db_state(None):
            assert self.sub.get_analysis_accessions() == {'sub_state_a1': 'ERZ0000001'}
            assert self.sub.get_analysis_aggregations() == {'sub_state_a1': 'basic'}

    def test_get_analysis_accessions_nothing_stored(self):
        with self.with_db_state(None):
            assert self.sub.get_analysis_accessions() == {}
            assert self.sub.get_analysis_aggregations() == {}

    def test_sample_name_to_accession_for_analysis_from_db(self):
        with self.with_db_state(DB_STATE):
            assert self.sub.sample_name_to_accession_for_analysis('sub_state_a1') == {'S1': 'SAME1', 'S2': 'SAME2'}

    def test_sample_name_to_accession_for_analysis_falls_back_on_config(self):
        self.sub.submission_cfg.set('brokering', 'Biosamples', 'sample_records', value=[
            SampleRecord('SAME9', analysis_aliases=['sub_state_a1'], sample_in_vcf='S9').to_dict()])
        with self.with_db_state(None):
            assert self.sub.sample_name_to_accession_for_analysis('sub_state_a1') == {'S9': 'SAME9'}


class TestPreparationState(SubmissionStateTestCase):
    submission_class = SubmissionPreparation

    def test_store_preparation_state(self):
        self.sub.submission_cfg.set('submission', 'metadata_json', value='/path/metadata.json')
        self.sub.submission_cfg.set('submission', 'project_title', value='A title')
        self.sub.submission_cfg.set('submission', 'taxonomy_id', value=9606)
        self.sub.submission_cfg.set('submission', 'analyses', value={
            'sub_state_a1': {'assembly_accession': 'GCA_000001405.15', 'vcf_files': ['/path/1.vcf.gz', '/path/2.vcf']},
            'sub_state_a2': {'assembly_accession': 'GCA_000001635.2', 'vcf_files': ['/path/3.vcf.gz']},
        })
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.store_preparation_state()
        m_update.assert_called_once_with(
            self.submission_id,
            metadata_json_file='/path/metadata.json',
            project_title='A title',
            project_taxonomies='9606',
            analyses=[
                {'analysisAlias': 'sub_state_a1', 'assemblyAccession': 'GCA_000001405.15',
                 'vcfs': [{'submittedFile': '/path/1.vcf.gz'}, {'submittedFile': '/path/2.vcf'}]},
                {'analysisAlias': 'sub_state_a2', 'assemblyAccession': 'GCA_000001635.2',
                 'vcfs': [{'submittedFile': '/path/3.vcf.gz'}]},
            ]
        )

    def test_store_preparation_state_without_taxonomy(self):
        self.sub.submission_cfg.set('submission', 'project_title', value='A title')
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.store_preparation_state()
        m_update.assert_called_once_with(self.submission_id, metadata_json_file=None, project_title='A title',
                                         project_taxonomies=None, analyses=[])


class TestValidationState(SubmissionStateTestCase):
    submission_class = SubmissionValidation

    def test_store_validation_state(self):
        self.sub.submission_cfg.set('validation', 'aggregation_check', 'analyses',
                                    value={'sub_state_a1': 'basic', 'sub_state_a2': 'none'})
        self.sub.submission_cfg.set('validation', 'valid', 'metadata_json', value='/path/metadata.json')
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.store_validation_state()
        m_update.assert_called_once_with(
            self.submission_id,
            metadata_json_file='/path/metadata.json',
            analyses=[{'analysisAlias': 'sub_state_a1', 'aggregationType': 'basic'},
                      {'analysisAlias': 'sub_state_a2', 'aggregationType': 'none'}]
        )

    def test_mark_valid_files_and_metadata_stores_state_when_validation_incomplete(self):
        self.sub.submission_cfg.set('validation', 'aggregation_check', 'analyses', value={'sub_state_a1': 'basic'})
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.mark_valid_files_and_metadata()
        # Nothing is valid yet so the metadata is not stored but the aggregation is
        m_update.assert_called_once_with(
            self.submission_id, metadata_json_file=None,
            analyses=[{'analysisAlias': 'sub_state_a1', 'aggregationType': 'basic'}])
        assert self.sub.submission_cfg.query('validation', 'valid', 'metadata_json') is None

    def test_mark_valid_files_and_metadata_stores_state_when_validation_complete(self):
        self.sub.submission_cfg.set('submission', 'metadata_json', value='/path/metadata.json')
        self.sub.submission_cfg.set('submission', 'analyses', value={
            'sub_state_a1': {'assembly_accession': 'GCA_1', 'vcf_files': ['/path/1.vcf.gz']}})
        self.sub.submission_cfg.set('validation', 'aggregation_check', 'analyses', value={'sub_state_a1': 'basic'})
        for task in self.sub.all_validation_tasks:
            self.sub.submission_cfg.set('validation', task, 'pass', value=True)
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.mark_valid_files_and_metadata()
        # The config is still written
        assert self.sub.submission_cfg.query('validation', 'valid', 'metadata_json') == '/path/metadata.json'
        m_update.assert_called_once_with(
            self.submission_id, metadata_json_file='/path/metadata.json',
            analyses=[{'analysisAlias': 'sub_state_a1', 'aggregationType': 'basic'}])

    def test_db_analyses_ignores_legacy_migrated_rows(self):
        state = {'analyses': [{'analysisAlias': 'ERZ0000009', 'analysisAccession': 'ERZ0000009'},
                              {'analysisAlias': 'sub_state_a1', 'analysisAccession': 'ERZ0000001'}]}
        with self.with_db_state(state):
            assert list(self.sub.db_analyses()) == ['sub_state_a1']
            assert self.sub.get_analysis_accessions() == {'sub_state_a1': 'ERZ0000001'}


class TestBrokeringState(SubmissionStateTestCase):
    submission_class = SubmissionBrokering

    def test_store_samples_state(self):
        records = [
            SampleRecord('SAME1', analysis_aliases=['a1'], sample_in_vcf='S1'),
            SampleRecord('SAME2', analysis_aliases=['a1', 'a2'], sample_in_vcf='S2'),
            SampleRecord('SAME3', analysis_aliases=['a2']),  # no name in the VCF: not stored
            SampleRecord(None, analysis_aliases=['a2'], sample_in_vcf='S4'),  # not accessioned: not stored
        ]
        with patch('eva_submission.submission.update_tracking_details') as m_update:
            self.sub.store_samples_state(records)
        m_update.assert_called_once_with(self.submission_id, analyses=[
            {'analysisAlias': 'sub_state_a1', 'samples': [{'sampleName': 'S1', 'sampleAccession': 'SAME1'},
                                                          {'sampleName': 'S2', 'sampleAccession': 'SAME2'}]},
            {'analysisAlias': 'sub_state_a2', 'samples': [{'sampleName': 'S2', 'sampleAccession': 'SAME2'}]},
        ])