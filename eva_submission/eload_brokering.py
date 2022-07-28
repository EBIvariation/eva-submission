import os
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import NEXTFLOW_DIR
from eva_submission.ENA_submission.upload_to_ENA import ENAUploader, ENAUploaderAsync
from eva_submission.biosamples_submission import SampleMetadataSubmitter, SampleReferenceSubmitter
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import read_md5
from eva_submission.submission_config import EloadConfig


class EloadBrokering(Eload):

    all_brokering_tasks = ['preparation', 'biosamples', 'ena', 'update_biosamples']

    def __init__(self, eload_number: int, vcf_files: list = None, metadata_file: str = None,
                 config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        if 'validation' not in self.eload_cfg:
            self.eload_cfg['validation'] = {}
        if vcf_files or metadata_file:
            self.eload_cfg.set('validation', 'valid', value={'Force': True, 'date': self.now})
            if vcf_files:
                self.eload_cfg.set('validation', 'valid', 'vcf_files', value=[os.path.abspath(v) for v in vcf_files])
            if metadata_file:
                self.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet', value=os.path.abspath(metadata_file))

    def broker(self, brokering_tasks_to_force=None, existing_project=None, async_upload=False, dry_ena_upload=False):
        """Run the brokering process"""
        self.eload_cfg.set('brokering', 'brokering_date', value=self.now)
        self.prepare_brokering(force=('preparation' in brokering_tasks_to_force))
        self.upload_to_bioSamples(force=('biosamples' in brokering_tasks_to_force))
        self.broker_to_ena(force=('ena' in brokering_tasks_to_force), existing_project=existing_project,
                           async_upload=async_upload, dry_ena_upload=dry_ena_upload)
        self.update_biosamples_with_study(force=('update_biosamples' in brokering_tasks_to_force))

    def prepare_brokering(self, force=False):
        valid_analyses = self.eload_cfg.query('validation', 'valid', 'analyses', ret_default=[])
        if not all([
            self.eload_cfg.query('brokering', 'analyses', analysis, 'vcf_files')
            for analysis in valid_analyses
        ]) or force:
            output_dir = self._run_brokering_prep_workflow()
            self._collect_brokering_prep_results(output_dir)
            shutil.rmtree(output_dir)
        else:
            self.info('Preparation has already been run, Skip!')

    def broker_to_ena(self, force=False, existing_project=None, async_upload=False, dry_ena_upload=False):
        if not self.eload_cfg.query('brokering', 'ena', 'PROJECT') or force:
            ena_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
            # Set the project in the metadata sheet which is then converted to XML
            self.update_metadata_spreadsheet(self.eload_cfg['validation']['valid']['metadata_spreadsheet'],
                                             ena_spreadsheet, existing_project)
            if async_upload:
                ena_uploader = ENAUploaderAsync(self.eload, ena_spreadsheet, self._get_dir('ena'))
            else:
                ena_uploader = ENAUploader(self.eload, ena_spreadsheet, self._get_dir('ena'))

            if ena_uploader.converter.is_existing_project:
                # Set the project in the config, based on the spreadsheet
                self.eload_cfg.set('brokering', 'ena', 'PROJECT', value=ena_uploader.converter.existing_project)
                self.eload_cfg.set('brokering', 'ena', 'existing_project', value=True)

            # Upload the VCF to ENA FTP
            files_to_upload = []
            analyses = self.eload_cfg['brokering']['analyses']
            for analysis in analyses:
                for vcf_file_name in analyses[analysis]['vcf_files']:
                    vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                    files_to_upload.append(vcf_file_info['output_vcf_file'])
                    files_to_upload.append(vcf_file_info['csi'])
            if not dry_ena_upload:
                ena_uploader.upload_vcf_files_to_ena_ftp(files_to_upload)
            else:
                self.info(f'Would have uploaded the following files to FTP: \n' + "\n".join(files_to_upload))

            # Upload XML to ENA
            ena_uploader.upload_xml_files_to_ena(dry_ena_upload)
            self.eload_cfg.set('brokering', 'ena', value=ena_uploader.results)
            self.eload_cfg.set('brokering', 'ena', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'ena', 'hold_date', value=ena_uploader.converter.hold_date)
            self.eload_cfg.set('brokering', 'ena', 'pass', value=not bool(ena_uploader.results['errors']))
        else:
            self.info('Brokering to ENA has already been run, Skip!')

    def upload_to_bioSamples(self, force=False):
        metadata_spreadsheet = self.eload_cfg['validation']['valid']['metadata_spreadsheet']
        sample_metadata_submitter = SampleMetadataSubmitter(metadata_spreadsheet)
        if sample_metadata_submitter.check_submit_done() and not force:
            self.info('Biosamples accession already provided in the metadata, Skip!')
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=True)
            # Retrieve the sample names to accession from the metadata
            sample_name_to_accession = sample_metadata_submitter.already_submitted_sample_names_to_accessions()
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
        elif (
            self.eload_cfg.query('brokering', 'Biosamples', 'Samples')
            and self.eload_cfg.query('brokering', 'Biosamples', 'pass')
            and not force
        ):
            self.info('BioSamples brokering is already done, Skip!')
        else:
            sample_name_to_accession = sample_metadata_submitter.submit_to_bioSamples()
            # Check whether all samples have been accessioned
            passed = (
                bool(sample_name_to_accession)
                and all(sample_name in sample_name_to_accession for sample_name in sample_metadata_submitter.all_sample_names())
            )
            self.eload_cfg.set('brokering', 'Biosamples', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=passed)
            # Make sure we crash if we haven't brokered everything
            if not passed:
                raise ValueError('Brokering to BioSamples failed!')

    def update_biosamples_with_study(self, force=False):
        if not self.eload_cfg.query('brokering', 'Biosamples', 'backlinks') or force:
            biosample_accession_list = self.eload_cfg.query('brokering', 'Biosamples', 'Samples').values()
            project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
            if project_accession:
                self.info(f'Add external reference to {len(biosample_accession_list)} BioSamples.')
                sample_reference_submitter = SampleReferenceSubmitter(biosample_accession_list, project_accession)
                sample_reference_submitter.submit_to_bioSamples()
                self.eload_cfg.set('brokering', 'Biosamples', 'backlinks', value=project_accession)
        else:
            self.info('Adding external reference to BioSamples has already been done, Skip!')

    def _get_valid_vcf_files(self):
        valid_vcf_files = []
        analyses = self.eload_cfg.query('validation', 'valid', 'analyses')
        for analysis_alias in analyses:
            files = analyses[analysis_alias]['vcf_files']
            valid_vcf_files.extend(files) if files else None
        return valid_vcf_files

    def _run_brokering_prep_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory()
        brokering_config = {
            'vcf_files': self._get_valid_vcf_files(),
            'output_dir': output_dir,
            'executable': cfg['executable']
        }
        # run the validation
        brokering_config_file = os.path.join(self.eload_dir, 'brokering_config_file.yaml')
        with open(brokering_config_file, 'w') as open_file:
            yaml.safe_dump(brokering_config, open_file)
        validation_script = os.path.join(NEXTFLOW_DIR, 'prepare_brokering.nf')
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
        valid_analyses = self.eload_cfg.query('validation', 'valid', 'analyses')
        for analysis in valid_analyses:
            analysis_config = {'assembly_accession': valid_analyses[analysis]['assembly_accession'],
                               'assembly_fasta': valid_analyses[analysis]['assembly_fasta'],
                               'assembly_report': valid_analyses[analysis]['assembly_report']}
            self.eload_cfg.set('brokering', 'analyses', analysis, value=analysis_config)
            vcf_files = valid_analyses[analysis]['vcf_files']
            for vcf_file in vcf_files:
                vcf_file_name = os.path.basename(vcf_file)
                if not vcf_file_name.endswith('.gz'):
                    vcf_file_name = vcf_file_name + '.gz'

                output_vcf_file = os.path.join(self._get_dir('ena'), vcf_file_name)
                os.rename(os.path.join(nextflow_vcf_output, vcf_file_name), output_vcf_file)
                os.rename(os.path.join(output_dir, vcf_file_name) + '.md5', output_vcf_file + '.md5')

                # .csi index is now supported by ENA
                csi_file = os.path.join(output_dir, vcf_file_name + '.csi')
                vcf_file_name_no_ext, ext = os.path.splitext(vcf_file_name)
                output_csi_file = os.path.join(self._get_dir('ena'), vcf_file_name_no_ext + '.csi')
                os.rename(csi_file, output_csi_file)
                os.rename(csi_file + '.md5', output_csi_file + '.md5')

                self.eload_cfg.set('brokering', 'analyses', analysis, 'vcf_files', output_vcf_file, value={
                    'original_vcf': vcf_file,
                    'output_vcf_file': output_vcf_file,
                    'md5': read_md5(output_vcf_file + '.md5'),
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
                                         for t in ['PROJECT', 'SUBMISSION', 'ANALYSIS'] if t in results]),
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
