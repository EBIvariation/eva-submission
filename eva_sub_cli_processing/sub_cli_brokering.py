from eva_sub_cli_processing.sub_cli_utils import BROKERING
from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_sub_cli_processing.sub_cli_submission import SubCliProcess


class SubCliProcessBrokering(SubCliProcess):

    processing_step = BROKERING

    def _start(self):
        """Run the brokering process"""
        self.upload_to_bioSamples()

    def upload_to_bioSamples(self, ):
        sample_submitter = SampleJSONSubmitter(('create',), metadata_json=self.submission_detail.get('metadataJson'))
        sample_name_to_accession = sample_submitter.submit_to_bioSamples()
        # Check whether all samples have been accessioned
        passed = (
            bool(sample_name_to_accession)
            and all(sample_name in sample_name_to_accession for sample_name in sample_submitter.all_sample_names())
        )
        if not passed:
            raise ValueError(f'Not all samples were successfully brokered to BioSamples! '
                             f'Found {len(sample_name_to_accession)} and expected '
                             f'{len(sample_submitter.all_sample_names())}. '
                             f'Missing samples are '
                             f'{[sample_name for sample_name in sample_submitter.all_sample_names() if sample_name not in sample_name_to_accession]}')
