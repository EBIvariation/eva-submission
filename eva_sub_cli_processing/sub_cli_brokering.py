from eva_sub_cli_processing.sub_cli_utils import BROKERING
from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_sub_cli_processing.sub_cli_submission import SubCliProcess


class SubCliProcessBrokering(SubCliProcess):

    processing_step = BROKERING

    def _start(self):
        """Run the brokering process"""
        self.upload_to_bioSamples()

    def upload_to_bioSamples(self, ):
        sample_submitter = SampleJSONSubmitter(self.submission_detail.get('metadataJson'), submit_type=('create',))
        sample_records = sample_submitter.submit_to_bioSamples()
        # Check whether all samples have been accessioned
        if not sample_submitter.check_sample_records(sample_records):
            raise ValueError(f'Not all samples were successfully brokered to BioSamples! '
                             f'Found {len(sample_records)} and expected {sample_submitter.nb_input_samples()}. '
                             f'Samples without accession are '
                             f'{[r.sample_in_vcf for r in sample_records if not r.accession]}')
