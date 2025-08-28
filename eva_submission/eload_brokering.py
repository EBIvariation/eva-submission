import csv
import json
import os
import shutil
import subprocess
import datetime

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import NEXTFLOW_DIR
from eva_submission.ENA_submission.json_to_ENA_json import EnaJsonConverter
from eva_submission.ENA_submission.upload_to_ENA import ENAUploader, ENAUploaderAsync
from eva_submission.biosample_submission.biosamples_submitters import SampleMetadataSubmitter, SampleReferenceSubmitter, \
    SampleJSONSubmitter
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import read_md5, get_nextflow_config_flag
from eva_submission.submission_config import EloadConfig


class EloadBrokering(Eload):

    all_brokering_tasks = ['preparation', 'biosamples', 'ena', 'update_biosamples']

    def __init__(self, eload_number: int, config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        if 'validation' not in self.eload_cfg:
            self.eload_cfg['validation'] = {}

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
        if not self.eload_cfg.query('brokering', 'ena', 'pass') or force:
            ena_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
            brokering_json = os.path.join(self._get_dir('ena'), 'metadata_json.json')
            metadata_json_file = self.eload_cfg.query('validation', 'valid', 'metadata_json')
            if metadata_json_file and os.path.exists(metadata_json_file):
                self.update_metadata_json(metadata_json_file, brokering_json, existing_project)
                metadata_for_ena = brokering_json
            else:
                self.update_metadata_spreadsheet(self.eload_cfg['validation']['valid']['metadata_spreadsheet'],
                                                 ena_spreadsheet, existing_project)
                metadata_for_ena = ena_spreadsheet
            if async_upload:
                ena_uploader = ENAUploaderAsync(self.eload, metadata_for_ena, self._get_dir('ena'))
            else:
                ena_uploader = ENAUploader(self.eload, metadata_for_ena, self._get_dir('ena'))

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
            if dry_ena_upload:
                self.info(f'Would have uploaded the following files to FTP: \n' + "\n".join(files_to_upload))
            else:
                ena_uploader.upload_vcf_files_to_ena_ftp(files_to_upload)
            # Upload metadata to ENA
            ena_uploader.upload_metadata_file_to_ena(dry_ena_upload)
            if not dry_ena_upload:
                # Update the project accession in case we're working with existing project
                # We should not be uploading additional analysis in th same ELOAD so no need to update
                pre_existing_project = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
                if pre_existing_project and 'PROJECT' not in ena_uploader.results:
                    ena_uploader.results['PROJECT'] = pre_existing_project
                self.eload_cfg.set('brokering', 'ena', value=ena_uploader.results)
                self.eload_cfg.set('brokering', 'ena', 'date', value=self.now)
                self.eload_cfg.set('brokering', 'ena', 'hold_date', value=ena_uploader.converter.hold_date)
                self.eload_cfg.set('brokering', 'ena', 'pass', value=not bool(ena_uploader.results['errors']))
        else:
            self.info('Brokering to ENA has already been run, Skip!')

    def upload_to_bioSamples(self, force=False):
        metadata_spreadsheet = self.eload_cfg.query('validation', 'valid', 'metadata_spreadsheet')
        metadata_json_file = self.eload_cfg.query('validation', 'valid', 'metadata_json')
        if metadata_json_file and os.path.exists(metadata_json_file):
            with open(metadata_json_file, 'r') as open_file:
                metadata_json = json.load(open_file)
                sample_submitter = SampleJSONSubmitter(metadata_json)
        elif metadata_spreadsheet and os.path.exists(metadata_spreadsheet):
            sample_submitter = SampleMetadataSubmitter(metadata_spreadsheet)
        else:
            self.error('No metadata spreadsheet or metadata json file present in the config')
            return

        if sample_submitter.check_submit_done():
            self.info('Biosamples accession already provided in the metadata, Skip!')
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=True)
            # Retrieve the sample names to accession from the metadata
            sample_name_to_accession = sample_submitter.already_submitted_sample_names_to_accessions()
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
        elif (
                self.eload_cfg.query('brokering', 'Biosamples', 'Samples')
            and self.eload_cfg.query('brokering', 'Biosamples', 'pass')
        ):
            self.info('BioSamples brokering is already done, Skip!')
        else:
            sample_name_to_accession = sample_submitter.submit_to_bioSamples()
            # Check whether all samples have been accessioned
            passed = (
                bool(sample_name_to_accession)
                and all(sample_name in sample_name_to_accession for sample_name in sample_submitter.all_sample_names())
            )
            self.eload_cfg.set('brokering', 'Biosamples', 'date', value=self.now)
            self.eload_cfg.set('brokering', 'Biosamples', 'Samples', value=sample_name_to_accession)
            self.eload_cfg.set('brokering', 'Biosamples', 'pass', value=passed)
            # Make sure we crash if we haven't brokered everything
            if not passed:
                raise ValueError(f'Not all samples were successfully brokered to BioSamples! '
                                 f'Found {len(sample_name_to_accession)} and expected '
                                 f'{len(sample_submitter.all_sample_names())}. '
                                 f'Missing samples are '
                                 f'{[sample_name for sample_name in sample_submitter.all_sample_names() if sample_name not in sample_name_to_accession]}')

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

    def _generate_csv_mappings(self):
        vcf_files_mapping_csv = os.path.join(self.eload_dir, 'brokering_vcf_files_mapping.csv')
        with open(vcf_files_mapping_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['vcf', 'fasta', 'report', 'assembly_accession'])
            sub_analyses = self.eload_cfg.query('submission', 'analyses')
            valid_analyses = self.eload_cfg.query('validation', 'valid', 'analyses')
            for analysis_alias in valid_analyses:
                fasta = sub_analyses[analysis_alias]['assembly_fasta']
                report = sub_analyses[analysis_alias]['assembly_report']
                assembly_accession = sub_analyses[analysis_alias]['assembly_accession']
                for vcf_file in valid_analyses[analysis_alias]['vcf_files']:
                    writer.writerow([vcf_file, fasta, report, assembly_accession])
        return vcf_files_mapping_csv

    def _run_brokering_prep_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory()
        cfg['executable']['python']['script_path'] = os.path.dirname(os.path.dirname(__file__))
        brokering_config = {
            'vcf_files_mapping': self._generate_csv_mappings(),
            'output_dir': output_dir,
            'executable': cfg['executable']
        }
        # run the brokering preparation
        brokering_config_file = os.path.join(self.eload_dir, 'brokering_config_file.yaml')
        with open(brokering_config_file, 'w') as open_file:
            yaml.safe_dump(brokering_config, open_file)
        brokering_script = os.path.join(NEXTFLOW_DIR, 'prepare_brokering.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow brokering preparation process',
                ' '.join((
                    cfg['executable']['nextflow'], brokering_script,
                    '-params-file', brokering_config_file,
                    '-work-dir', output_dir,
                    get_nextflow_config_flag()
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('Nextflow pipeline failed: aborting brokering')
            raise e
        return output_dir

    def parse_bcftools_norm_report(self, norm_report):
        total = split = realigned = skipped = 0
        error_list = []
        with open(norm_report) as open_file:
            for line in open_file:
                if line.startswith('Lines   total/split/realigned/skipped:'):
                    # Lines   total/split/realigned/skipped:  2/0/1/0
                    total, split, realigned, skipped = line.strip().split()[-1].split('/')
                else:
                    error_list.append(line.strip())
        return error_list, int(total), int(split), int(realigned), int(skipped)

    def _collect_brokering_prep_results(self, output_dir):
        # Collect information from the output and summarise in the config
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
                os.rename(os.path.join(output_dir, vcf_file_name), output_vcf_file)
                os.rename(os.path.join(output_dir, vcf_file_name) + '.md5', output_vcf_file + '.md5')

                # .csi index is now supported by ENA
                csi_file = os.path.join(output_dir, vcf_file_name + '.csi')
                vcf_file_name_no_ext, ext = os.path.splitext(vcf_file_name)
                output_csi_file = os.path.join(self._get_dir('ena'), vcf_file_name_no_ext + '.csi')
                os.rename(csi_file, output_csi_file)
                os.rename(csi_file + '.md5', output_csi_file + '.md5')

                uncompressed_vcf_name = vcf_file_name[:-3]
                output_norm_log = os.path.join(self._get_dir('ena'), uncompressed_vcf_name + '_bcftools_norm.log')
                os.rename(os.path.join(output_dir, uncompressed_vcf_name + '_bcftools_norm.log'), output_norm_log)

                error_list, total, split, realigned, skipped = self.parse_bcftools_norm_report(output_norm_log)
                normalisation_stat = {
                    'error_list': error_list, 'nb_variant': total, 'nb_split': split,
                    'nb_realigned': realigned, 'nb_skipped': skipped,
                }
                self.eload_cfg.set('brokering', 'analyses', analysis, 'vcf_files', output_vcf_file, value={
                    'original_vcf': vcf_file,
                    'output_vcf_file': output_vcf_file,
                    'md5': read_md5(output_vcf_file + '.md5'),
                    'csi': output_csi_file,
                    'csi_md5': read_md5(output_csi_file + '.md5'),
                    'normalisation': normalisation_stat
                })

    def _normalisation_check_report(self):
        reports = []
        for vcf_file in self.eload_cfg.query('validation', 'normalisation_check', 'files', ret_default=[]):
            results = self.eload_cfg.query('validation', 'normalisation_check', 'files', vcf_file)
            report_data = {
                'vcf_file': vcf_file,
                'pass': 'PASS' if len(results.get('error_list')) == 0 else 'FAIL',
                'nb_error': len(results.get('error_list')),
                'errors': '\n'.join(results['error_list']),
            }
            report_data.update(results)
            reports.append("""  * {vcf_file}: {pass}
    - number of error: {nb_error}
    - nb of variant: {nb_variant}
    - nb of normalised: {nb_realigned}
    - see report for detail: {normalisation_log}""".format(**report_data))
        return '\n'.join(reports)

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
    
    def _archival_confirmation_text(self):
        if not self._brokering_complete():
            return 'NA'
        study_title = self.eload_cfg.query('submission', 'project_title')
        hold_date = self.eload_cfg.query('brokering', 'ena', 'hold_date')
        brokering_date_from_config = self.eload_cfg.query('brokering', 'brokering_date')
        try:
            brokering_date = datetime.datetime.strptime(brokering_date_from_config.split(" ")[0].strip(), "%Y-%m-%d").date()
        except:
            brokering_date = datetime.date.today()
        brokering_date_plus_3 = brokering_date + datetime.timedelta(days=3)
        available_date = hold_date if hold_date is not None else brokering_date_plus_3
        if isinstance(available_date, datetime.datetime) or isinstance(available_date, datetime.date):
            available_date_str = available_date.strftime("%Y-%m-%d")
        else:
            available_date_str = available_date.split(" ")[0]
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        analysis_accession = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS', ret_default={})

        taxonomy_id = self.eload_cfg.query('submission', 'taxonomy_id')
        non_human_study_text = 'Please allow at least 48 hours from the initial release date provided for the data to be made available through this link. Each variant will be issued a unique SS# ID which will be made available to download via the "browsable files" link on the EVA study page.' if taxonomy_id!=9606 else ""

        archival_text_data = {
            'study_title': study_title,
            'available_date': available_date_str,
            'project_accession': project_accession,
            'analysis_accession': ', '.join([
                f'{self._undo_unique_alias(alias)}=>{accession}'
                for alias, accession in analysis_accession.items()
            ]),
            'non_human_study': non_human_study_text
        }
        
        archival_text = """
Your EVA submission "{study_title}" has now been archived and will be made available to the public on {available_date}. The accessions associated with your submission are:
Project: {project_accession}
Analyses: {analysis_accession} 
If you wish your data to be held private beyond the date specified above, please let us know. Once released, the data will be made available to download from this link: https://www.ebi.ac.uk/eva/?eva-study={project_accession}
{non_human_study}
You can also notify us when your paper has been assigned a PMID. We will add this to your study page in the EVA. If there is anything else you need please do not hesitate to notify me. Archived data can be referenced using the project accession & associated URL e.g. The variant data for this study have been deposited in the European Variation Archive (EVA) at EMBL-EBI under accession number {project_accession} (https://www.ebi.ac.uk/eva/?eva-study={project_accession})
The EVA can be cited directly using the associated literature:
Cezard T, Cunningham F, Hunt SE, Koylass B, Kumar N, Saunders G, Shen A, Silva AF, Tsukanov K, Venkataraman S, Flicek P, Parkinson H, Keane TM. The European Variation Archive: a FAIR resource of genomic variation for all species. Nucleic Acids Res. 2021 Oct 28:gkab960. doi: 10.1093/nar/gkab960. PMID: 34718739.
"""
        
        return archival_text.format(**archival_text_data)

    def report(self):
        """Collect information from the config and write the report."""
        report_data = {
            'brokering_date': self.eload_cfg.query('brokering', 'brokering_date'),
            'biosamples_status': self._check_pass_or_fail(self.eload_cfg.query('brokering', 'Biosamples')),
            'ena_status': self._check_pass_or_fail(self.eload_cfg.query('brokering', 'ena')),
            'biosamples_report': self._biosamples_report(),
            'ena_report': self._ena_report(),
            'archival_confirmation_text': self._archival_confirmation_text()
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
----------------------------------

Archival Confirmation Text:
{archival_confirmation_text}
"""
        print(report.format(**report_data))

    def _brokering_complete(self):
        return all([self.eload_cfg.query('brokering', key, 'pass') for key in ['ena', 'Biosamples']])