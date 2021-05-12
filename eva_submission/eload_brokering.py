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
from eva_submission.ENA_submission.xlsx_to_ENA_xml import EnaXlsxConverter


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

    def broker(self, brokering_tasks_to_force=None):
        """Run the brokering process"""
        self.eload_cfg.set('brokering', 'brokering_date', value=self.now)
        self.prepare_brokering(force=('preparation' in brokering_tasks_to_force))
        self.upload_to_bioSamples(force=('biosamples' in brokering_tasks_to_force))
        self.broker_to_ena(force=('ena' in brokering_tasks_to_force))

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
            converter = EnaXlsxConverter(ena_spreadsheet, self._get_dir('ena'), self.eload)
            submission_file, project_file, analysis_file = converter.create_submission_files()

            # Upload the VCF to ENA FTP
            ena_uploader = ENAUploader(self.eload)
            files_to_upload = [vcf_file for vcf_file in self.eload_cfg['brokering']['vcf_files']] + \
                              [self.eload_cfg['brokering']['vcf_files'][vcf_file]['index'] for vcf_file in self.eload_cfg['brokering']['vcf_files']]
            ena_uploader.upload_vcf_files_to_ena_ftp(files_to_upload)

            # Upload XML to ENA
            ena_uploader.upload_xml_files_to_ena(submission_file, project_file, analysis_file)
            self.eload_cfg.set('brokering', 'ena', value=ena_uploader.results)
            self.eload_cfg.set('brokering', 'ena', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'ena', 'hold_date', value=converter.hold_date)
            self.eload_cfg.set('brokering', 'ena', 'pass', value=not bool(ena_uploader.results['errors']))
        else:
            self.info('Brokering to ENA has already been run, Skip!')

    def upload_to_bioSamples(self, force=False):
        metadata_spreadsheet = self.eload_cfg['validation']['valid']['metadata_spreadsheet']
        sample_tab_submitter = SampleMetadataSubmitter(metadata_spreadsheet)
        if sample_tab_submitter.check_submit_done() and not force:
            self.info('Biosamples accession already provided in the metadata, Skip!')
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=True)
        elif self.eload_cfg.query('brokering', 'Biosamples', 'Samples') and not force:
            self.info('BioSamples brokering is already done, Skip!')
        else:
            sample_name_to_accession = sample_tab_submitter.submit_to_bioSamples()
            self.eload_cfg.set('brokering', 'Biosamples', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=bool(sample_name_to_accession))

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
                'Nextflow brokering preparation process',
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

            # .csi index not supported by ENA, so we just save it to be made public later
            csi_file = os.path.join(output_dir, vcf_file_name + '.csi')
            output_csi_file = os.path.join(self._get_dir('ena'), vcf_file_name + '.csi')
            os.rename(csi_file, output_csi_file)
            os.rename(csi_file + '.md5', output_csi_file + '.md5')

            self.eload_cfg.set('brokering', 'vcf_files', output_vcf_file, value={
                'original_vcf': vcf_file,
                'md5': read_md5(output_vcf_file + '.md5'),
                'index': output_index_file,
                'index_md5': read_md5(output_index_file + '.md5'),
                'csi': output_csi_file,
                'csi_md5': read_md5(output_csi_file + '.md5')
            })

    def _biosamples_report(self):
        reports = []
        results = self.eload_cfg.query('brokering', 'Biosamples', ret_default={})
        report_data = {
            'samples_to_accessions': '\n'.join(['%s: %s' % (s, a) for s, a in results.get('Samples',  {}).items()]),
            'pass': 'PASS' if results.get('pass') else 'FAIL'
        }
        reports.append("""  * Biosamples: {pass}
    - Accessions: {samples_to_accessions}
""".format(**report_data))
        return '\n'.join(reports)

    def _ena_report(self):
        reports = []
        results = self.eload_cfg.query('brokering', 'ena', ret_default={})
        report_data = {
            'ena_accessions': '\n'.join(['%s: %s' % (t, results.get(t))
                                         for t in ['PROJECT','SUBMISSION', 'ANALYSIS'] if t in results]),
            'hold_date': results.get('hold_date', ''),
            'pass': 'PASS' if results.get('pass') else 'FAIL',
            'errors': '\n'.join(results.get('errors', [])),
            'receipt': results.get('receipt', '')
        }
        reports.append("""  * ENA: {pass}
    - Hold date: {hold_date}
    - Accessions: {ena_accessions}
    - Errors: {errors}
    - receipt: {receipt}
""".format(**report_data))
        return '\n'.join(reports)

    def report(self):
        """Collect information from the config and write the report."""
        report_data = {
            'brokering_date': self.eload_cfg.query('brokering', 'brokering_date'),
            'biosamples_status': self._check_pass_or_fail(self.eload_cfg.query('brokering', 'Biosamples')),
            'ena_status': self._check_pass_or_fail(self.eload_cfg.query('brokering', 'ena')),
            'biosamples_report': self._biosamples_report(),
            'ena_report': self._ena_report(),

        }
        report = """Brokering performed on {brokering_date}
BioSamples: {biosamples_status}
ENA: {ena_status}
----------------------------------

BioSamples brokering:
{biosamples_report}
----------------------------------

ENA brokering:
{ena_report}
----------------------------------"""
        print(report.format(**report_data))
