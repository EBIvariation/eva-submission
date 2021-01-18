import os
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.ENA_submission.upload_to_ENA import ENAUploader
from eva_submission.biosamples_submission import SampleMetadataSubmitter
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import read_md5
from eva_submission.ENA_submission.xlsx_to_ENA_xml import process_metadata_spreadsheet


class EloadBrokering(Eload):

    all_brokering_tasks = ['preparation', 'biosamples', 'ena']

    def __init__(self, eload_number: int, vcf_files: list = None, metadata_file: str = None):
        super().__init__(eload_number)
        if 'validation' not in self.eload_cfg:
            self.eload_cfg['validation'] = {}
        if vcf_files or metadata_file:
            self.eload_cfg.set('validation', 'valid', value={'Force': True, 'date': self.now})
            if vcf_files:
                self.eload_cfg.set('validation', 'valid', 'vcf_files', value=[os.path.abspath(v) for v in vcf_files])
            if metadata_file:
                self.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet', value=os.path.abspath(metadata_file))

    def broker(self, brokering_tasks=None):
        """Run the brokering process"""
        self.prepare_brokering(force='preparation' in brokering_tasks)
        self.upload_to_bioSamples(force='biosamples' in brokering_tasks)
        self.broker_to_ena(force='ena' in brokering_tasks)

    def prepare_brokering(self, force=False):
        if not self.eload_cfg.query('brokering', 'vcf_files') or force:
            output_dir = self._run_brokering_prep_workflow()
            self._collect_brokering_prep_results(output_dir)
            shutil.rmtree(output_dir)
        else:
            self.info('Preparation has already been run, Skip!')

    def broker_to_ena(self, force=False):
        if not self.eload_cfg.query('brokering', 'ena', 'PROJECT') or force:
            ena_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
            self.update_metadata_from_config(self.eload_cfg['validation']['valid']['metadata_spreadsheet'], ena_spreadsheet)
            submission_file, project_file, analysis_file = process_metadata_spreadsheet(ena_spreadsheet,
                                                                                        self._get_dir('ena'), self.eload)
            # Upload the VCF to ENA FTP
            ena_uploader = ENAUploader(self.eload)
            files_to_upload = [vcf_file for vcf_file in self.eload_cfg['brokering']['vcf_files']] + \
                              [self.eload_cfg['brokering']['vcf_files'][vcf_file]['index'] for vcf_file in self.eload_cfg['brokering']['vcf_files']]
            ena_uploader.upload_vcf_files_to_ena_ftp(files_to_upload)

            # Upload XML to ENA
            ena_uploader.upload_xml_files_to_ena(submission_file, project_file, analysis_file)
            self.eload_cfg.set('brokering', 'ena', value=ena_uploader.results)

    def upload_to_bioSamples(self, force=False):
        metadata_spreadsheet = self.eload_cfg['validation']['valid']['metadata_spreadsheet']
        sample_tab_submitter = SampleMetadataSubmitter(metadata_spreadsheet)
        if not (sample_tab_submitter.check_submit_done() or self.eload_cfg.query('brokering', 'Biosamples', 'Samples'))\
                or force:
            sample_name_to_accession = sample_tab_submitter.submit_to_bioSamples()
            self.eload_cfg.set('brokering', 'Biosamples', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
        else:
            self.info('BioSamples brokering is already done, Skip!')

    def _run_brokering_prep_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory()
        brokering_config = {
            'vcf_files': self.eload_cfg.query('validation', 'valid', 'vcf_files'),
            'output_dir': output_dir,
            'executable': cfg['executable']
        }
        # run the validation
        brokering_config_file = os.path.join(self.eload_dir, 'brokering_config_file.yaml')
        with open(brokering_config_file, 'w') as open_file:
            yaml.safe_dump(brokering_config, open_file)
        validation_script = os.path.join(ROOT_DIR, 'nextflow', 'prepare_brokering.nf')
        try:
            command_utils.run_command_with_output(
                'Start Nextflow brokering preparation process',
                ' '.join((
                    cfg['executable']['nextflow'], validation_script,
                    '-params-file', brokering_config_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('Nextflow pipeline failed: aborting brokering')
            raise e
        return output_dir

    def _collect_brokering_prep_results(self, output_dir):
        # Collect information from the output and summarise in the config
        nextflow_vcf_output = os.path.join(output_dir, 'output')
        for vcf_file in self.eload_cfg.query('validation', 'valid', 'vcf_files'):
            vcf_file_name = os.path.basename(vcf_file)
            if not vcf_file_name.endswith('.gz'):
                vcf_file_name = vcf_file_name + '.gz'

            output_vcf_file = os.path.join(self._get_dir('ena'), vcf_file_name)
            os.rename(os.path.join(nextflow_vcf_output, vcf_file_name), output_vcf_file)
            os.rename(os.path.join(output_dir, vcf_file_name) + '.md5', output_vcf_file + '.md5')

            index_file = os.path.join(output_dir, vcf_file_name + '.tbi')
            output_index_file = os.path.join(self._get_dir('ena'), vcf_file_name + '.tbi')
            os.rename(index_file, output_index_file)
            os.rename(index_file + '.md5', output_index_file + '.md5')

            self.eload_cfg.set('brokering', 'vcf_files', output_vcf_file, value={
                'original_vcf': vcf_file,
                'md5': read_md5(output_vcf_file+'.md5'),
                'index': output_index_file,
                'index_md5': read_md5(output_index_file+'.md5'),
            })
