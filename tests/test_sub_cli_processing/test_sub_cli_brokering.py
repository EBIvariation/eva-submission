from unittest import TestCase
from unittest.mock import patch, PropertyMock

from eva_sub_cli_processing.sub_cli_brokering import SubCliProcessBrokering
from eva_sub_cli_processing.sub_cli_submission import SubCliProcess
from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_submission.biosample_submission.sample_record import SampleRecord


class TestSubCliProcessBrokering(TestCase):

    metadata_json = {'sample': [
        {'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000001'},
        {'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000002'},
    ]}

    def _upload_to_biosamples(self, sample_records):
        # Avoid the creation of the submission directory
        with patch.object(SubCliProcess, '__init__', return_value=None), \
                patch.object(SubCliProcessBrokering, 'submission_detail', new_callable=PropertyMock,
                             return_value={'metadataJson': self.metadata_json}), \
                patch.object(SampleJSONSubmitter, 'submit_to_bioSamples', return_value=sample_records):
            SubCliProcessBrokering('submission1').upload_to_bioSamples()

    def test_upload_to_bioSamples(self):
        self._upload_to_biosamples([
            SampleRecord('SAME000001', analysis_aliases=['AF1'], name='bsd1'),
            SampleRecord('SAME000002', analysis_aliases=['AF1'], name='bsd2'),
        ])

    def test_upload_to_bioSamples_incomplete(self):
        with self.assertRaises(ValueError):
            self._upload_to_biosamples([SampleRecord('SAME000001', analysis_aliases=['AF1'], name='bsd1')])

    def test_upload_to_bioSamples_sample_without_accession(self):
        with self.assertRaises(ValueError) as context:
            self._upload_to_biosamples([
                SampleRecord('SAME000001', analysis_aliases=['AF1'], name='bsd1'),
                SampleRecord(None, analysis_aliases=['AF1'], sample_in_vcf='S2'),
            ])
        assert "['S2']" in str(context.exception)
