from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import biosamples_submission
from eva_submission.eload_submission import Eload


class EloadBrokering(Eload):

    def run_metadata_parser(self, output_folder, metadata_file):
        command = '{perl} {submission_to_xml} -f {output} -r {eload} -i ${metadata_file}'.format(
            perl=cfg['executable']['perl'], submission_to_xml=cfg['executable']['submission_to_xml'],
            output=output_folder, eload=self.eload, metadata_file=metadata_file
        )
        command_utils.run_command_with_output('Run metadata perl scripts', command)


    def upload_to_bioSamples(self, sample_tab):
        accessionned_sampletab = biosamples_submission.submit_to_bioSamples(sample_tab)

