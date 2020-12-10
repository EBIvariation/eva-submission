#!/usr/bin/env python
import glob
import os
import shutil
import string
import random
from datetime import datetime

import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl
from ebi_eva_common_pyutils.variation.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi

from eva_submission.eload_utils import get_genome_fasta_and_report
from eva_submission.submission_config import EloadConfig
from eva_submission.submission_in_ftp import FtpDepositBox
from eva_submission.xls_parser_eva import EVAXLSReader

directory_structure = {
    'vcf': '10_submitted/vcf_files',
    'metadata': '10_submitted/metadata_file',
    'validation': '13_validation',
    'vcf_check': '13_validation/vcf_format',
    'assembly_check': '13_validation/assembly_check',
    'sample_check': '13_validation/sample_concordance',
    'biosamles': '18_brokering/biosamples',
    'ena': '18_brokering/ena',
    'scratch': '20_scratch'
}


class Eload(AppLogger):
    def __init__(self, eload_number: int):
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.eload_cfg = EloadConfig(os.path.join(self.eload_dir, '.' + self.eload + '_config.yml'))

        os.makedirs(self.eload_dir, exist_ok=True)
        for k in directory_structure:
            os.makedirs(self._get_dir(k), exist_ok=True)

    def create_temp_output_directory(self):
        random_string = ''.join(random.choice(string.ascii_letters) for i in range(6))
        output_dir = os.path.join(self.eload_dir, 'nextflow_output_' + random_string)
        os.makedirs(output_dir)
        return output_dir

    def _get_dir(self, key):
        return os.path.join(self.eload_dir, directory_structure[key])

    @cached_property
    def now(self):
        return datetime.now()


class EloadPreparation(Eload):

    def copy_from_ftp(self, ftp_box, submitter):
        box = FtpDepositBox(ftp_box, submitter)

        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        for vcf_file in box.vcf_files:
            dest = os.path.join(vcf_dir, os.path.basename(vcf_file))
            shutil.copyfile(vcf_file, dest)

        if box.most_recent_metadata:
            if len(box.metadata_files) != 1:
                self.warning('Found %s metadata file in the FTP. Will use the most recent one', len(box.metadata_files))
            metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
            dest = os.path.join(metadata_dir, os.path.basename(box.most_recent_metadata))
            shutil.copyfile(box.most_recent_metadata, dest)
        else:
            self.error('No metadata file in the FTP: %s', box.deposit_box)

        for other_file in box.other_files:
            self.warning('File %s will not be treated', other_file)

    def detect_all(self):
        self.detect_submitted_metadata()
        self.detect_submitted_vcf()
        self.detect_metadata_attibutes()
        self.find_genome()

    def detect_submitted_metadata(self):
        metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
        metadata_spreadsheets = glob.glob(os.path.join(metadata_dir, '*.xlsx'))
        if len(metadata_spreadsheets) != 1:
            self.critical('Found %s spreadsheet in %s', len(metadata_spreadsheets), metadata_dir)
            raise ValueError('Found %s spreadsheet in %s'% (len(metadata_spreadsheets), metadata_dir))
        self.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata_spreadsheets[0])

    def detect_submitted_vcf(self):
        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        uncompressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf'))
        compressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf.gz'))
        vcf_files = uncompressed_vcf + compressed_vcf
        if len(vcf_files) < 1:
            raise FileNotFoundError('Could not locate vcf file in in %s', vcf_dir)
        self.eload_cfg.set('submission','vcf_files', value=vcf_files)

    def detect_metadata_attibutes(self):
        eva_metadata = EVAXLSReader(self.eload_cfg.query('submission', 'metadata_spreadsheet'))
        reference_gca = set()
        for analysis in eva_metadata.analysis:
            reference_txt = analysis.get('Reference')
            reference_gca.update(retrieve_genbank_assembly_accessions_from_ncbi(reference_txt))

        if len(reference_gca) > 1:
            self.warning('Multiple assemblies in project: %s', ', '.join(reference_gca))
            self.warning('Will look for the most recent assembly.')
            reference_gca = [sorted(reference_gca)[-1]]

        if reference_gca:
            self.eload_cfg.set('submission', 'assembly_accession', value=reference_gca.pop())
        else:
            self.error('No genbank accession could be found for %s', reference_txt)

        taxonomy_id = eva_metadata.project.get('Tax ID')
        if taxonomy_id and (isinstance(taxonomy_id, int) or taxonomy_id.isdigit()):
            self.eload_cfg.set('submission', 'taxonomy_id', value=int(taxonomy_id))
            scientific_name = get_scientific_name_from_ensembl(taxonomy_id)
            self.eload_cfg.set('submission', 'scientific_name', value=scientific_name)
        else:
            if taxonomy_id:
                self.error('Taxonomy id %s is invalid:', taxonomy_id)
            else:
                self.error('Taxonomy id is missing for the submission')

    def find_genome(self):
        assembly_fasta_path, assembly_report_path = get_genome_fasta_and_report(
            self.eload_cfg.query('submission', 'scientific_name'),
            self.eload_cfg.query('submission', 'assembly_accession')
        )
        self.eload_cfg.set('submission', 'assembly_fasta', value=assembly_fasta_path)
        self.eload_cfg.set('submission', 'assembly_report', value=assembly_report_path)


class EloadValidation(Eload):

    @cached_property
    def now(self):
        return datetime.now()

    def create_temp_output_directory(self):
        random_string = ''.join(random.choice(string.ascii_letters) for i in range(6))
        output_dir = os.path.join(self.eload_dir, 'nextflow_output_' + random_string)
        os.makedirs(output_dir)
        return output_dir

    def validate(self):
        # (Re-)Initialise the config file output
        self.eload_cfg['validation'] = {
            'validation_date': self.now,
            'assembly_check': {},
            'vcf_check': {},
            'sample_check': {}
        }
        self._validate_spreadsheet()
        output_dir = self._run_validation_workflow()
        self._collect_validation_worklflow_results(output_dir)
        shutil.rmtree(output_dir)

    def _validate_spreadsheet(self):
        overall_differences, results_per_analysis_alias = compare_spreadsheet_and_vcf(
            eva_files_sheet=self.eload_cfg.query('submission', 'metadata_spreadsheet'),
            vcf_dir=self._get_dir('vcf'),
            expected_vcf_files=self.eload_cfg['submission']['vcf_files']
        )
        self.eload_cfg['validation']['sample_check']['analysis'] = {}
        for analysis_alias in results_per_analysis_alias:
            has_difference, diff_submitted_file_submission, diff_submission_submitted_file = results_per_analysis_alias[analysis_alias]

            self.eload_cfg['validation']['sample_check']['analysis'][str(analysis_alias)] = {
                'difference_exists': has_difference,
                'in_VCF_not_in_metadata': diff_submitted_file_submission,
                'in_metadata_not_in_VCF': diff_submission_submitted_file
            }
        self.eload_cfg['validation']['sample_check']['pass'] = not overall_differences


    def parse_assembly_check_log(self, assembly_check_log):
        error_list = []
        nb_error = 0
        match = total = None
        with open(assembly_check_log) as open_file:
            for line in open_file:
                if line.startswith('[error]'):
                    nb_error += 1
                    if nb_error < 11:
                        error_list.append(line.strip()[len('[error]'):])
                elif line.startswith('[info] Number of matches:'):
                    match, total = line.strip()[len('[info] Number of matches: '):].split('/')
                    match = int(match)
                    total = int(total)
        return error_list, nb_error, match, total

    def parse_vcf_check_report(self, vcf_check_report):
        valid = True
        error_list = []
        warning_count = error_count = 0
        with open(vcf_check_report) as open_file:
            for line in open_file:
                if 'warning' in line:
                    warning_count = 1
                elif line.startswith('According to the VCF specification'):
                    if 'not' in line:
                        valid = False
                else:
                    error_count += 1
                    if error_count < 11:
                        error_list.append(line.strip())
        return valid, error_list, error_count, warning_count

    def _run_validation_workflow(self):
        output_dir = self.create_temp_output_directory()
        validation_config = {
            'metadata_file': self.eload_cfg.query('submission', 'metadata_spreadsheet'),
            'vcf_files': self.eload_cfg.query('submission', 'vcf_files'),
            'reference_fasta': self.eload_cfg.query('submission', 'assembly_fasta'),
            'reference_report': self.eload_cfg.query('submission', 'assembly_report'),
            'output_dir': output_dir,
            'executable': cfg['executable']
        }
        # run the validation
        validation_confg_file = os.path.join(self.eload_dir, 'validation_confg_file.yaml')
        with open(validation_confg_file, 'w') as open_file:
            yaml.safe_dump(validation_config, open_file)
        validation_script = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nextflow', 'validation.nf')
        try:
            command_utils.run_command_with_output(
                'Start Nextflow Validation process',
                ' '.join((
                    cfg['executable']['nextflow'], validation_script,
                    '-params-file', validation_confg_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError:
            self.error('Nextflow pipeline failed: results might not be complete')
        return output_dir

    def _move_file(self, source, dest):
        if source:
            self.debug('Rename %s to %s', source, dest)
            os.rename(source, dest)
            return dest
        else:
            return None

    def _collect_validation_worklflow_results(self, output_dir):
        # Collect information from the output and summarise in the config
        self.eload_cfg['validation']['vcf_check']['files'] = {}
        total_error = 0
        # detect output files for vcf check
        for vcf_file in self.eload_cfg.query('submission', 'vcf_files'):
            vcf_name = os.path.basename(vcf_file)

            tmp_vcf_check_log = resolve_single_file_path(
                os.path.join(output_dir, 'vcf_format', vcf_name + '.vcf_format.log')
            )
            tmp_vcf_check_text_report = resolve_single_file_path(
                os.path.join(output_dir, 'vcf_format', vcf_name + '.*.txt')
            )
            tmp_vcf_check_db_report = resolve_single_file_path(
                os.path.join(output_dir, 'vcf_format', vcf_name + '.*.db')
            )

            # move the output files
            vcf_check_log = self._move_file(
                tmp_vcf_check_log,
                os.path.join(self._get_dir('vcf_check'), vcf_name + '.vcf_format.log')
            )
            vcf_check_text_report = self._move_file(
                tmp_vcf_check_text_report,
                os.path.join(self._get_dir('vcf_check'), vcf_name + '.vcf_validator.txt')
            )
            vcf_check_db_report = self._move_file(
                tmp_vcf_check_db_report,
                os.path.join(self._get_dir('vcf_check'), vcf_name + '.vcf_validator.db')
            )
            if vcf_check_log and vcf_check_text_report and vcf_check_db_report:
                valid, error_list, error_count, warning_count = self.parse_vcf_check_report(vcf_check_text_report)
            else:
                valid, error_list, error_count, warning_count = (False, ['Process failed', 1, 0])
            total_error += error_count

            self.eload_cfg['validation']['vcf_check']['files'][vcf_name] = {
                'error_list': error_list, 'nb_error': error_count, 'nb_warning': warning_count,
                'vcf_check_log': vcf_check_log, 'vcf_check_text_report': vcf_check_text_report,
                'vcf_check_db_report': vcf_check_db_report
            }
        self.eload_cfg['validation']['vcf_check']['pass'] = total_error == 0

        # detect output files for assembly check
        self.eload_cfg['validation']['assembly_check']['files'] = {}
        total_error = 0
        for vcf_file in self.eload_cfg.query('submission', 'vcf_files'):
            vcf_name = os.path.basename(vcf_file)

            tmp_assembly_check_log = resolve_single_file_path(
                os.path.join(output_dir, 'assembly_check',  vcf_name + '.assembly_check.log')
            )
            tmp_assembly_check_valid_vcf = resolve_single_file_path(
                os.path.join(output_dir, 'assembly_check', vcf_name + '.valid_assembly_report*')
            )
            tmp_assembly_check_text_report = resolve_single_file_path(
                os.path.join(output_dir, 'assembly_check', vcf_name + '*text_assembly_report*')
            )

            # move the output files
            assembly_check_log = self._move_file(
                tmp_assembly_check_log,
                os.path.join(self._get_dir('assembly_check'), vcf_name + '.assembly_check.log')
            )
            assembly_check_valid_vcf = self._move_file(
                tmp_assembly_check_valid_vcf,
                os.path.join(self._get_dir('assembly_check'), vcf_name + '.valid_assembly_report.txt')
            )
            assembly_check_text_report = self._move_file(
                tmp_assembly_check_text_report,
                os.path.join(self._get_dir('assembly_check'), vcf_name + '.text_assembly_report.txt')
            )
            if assembly_check_log and assembly_check_valid_vcf and assembly_check_text_report:
                error_list, nb_error, match, total = self.parse_assembly_check_log(assembly_check_log)
            else:
                error_list, nb_error, match, total = (['Process failed'], 1, 0, 0)
            total_error += nb_error
            self.eload_cfg['validation']['assembly_check']['files'][vcf_name] = {
                'error_list': error_list, 'nb_error': nb_error, 'ref_match': match, 'nb_variant': total,
                'assembly_check_log': assembly_check_log, 'assembly_check_valid_vcf': assembly_check_valid_vcf,
                'assembly_check_text_report': assembly_check_text_report
            }
        self.eload_cfg['validation']['assembly_check']['pass'] = total_error == 0

    def _vcf_check_report(self):
        reports = []
        for vcf_file in self.eload_cfg.query('validation', 'vcf_check', 'files'):
            results = self.eload_cfg.query('validation', 'vcf_check', 'files', vcf_file)
            report_data = {
                'vcf_file': vcf_file,
                'pass': 'PASS' if results.get('nb_error') == 0 else 'FAIL',
                '10_error_list': '\n'.join(results['error_list'])
            }
            report_data.update(results)
            reports.append("""  * {vcf_file}: {pass}
    - number of error: {nb_error}
    - number of warning: {nb_warning}
    - first 10 errors: {10_error_list}
    - see report for detail: {vcf_check_text_report}
""".format(**report_data))
        return '\n'.join(reports)

    def _assembly_check_report(self):
        reports = []
        for vcf_file in self.eload_cfg.query('validation', 'assembly_check', 'files'):
            results = self.eload_cfg.query('validation', 'assembly_check', 'files', vcf_file)
            report_data = {
                'vcf_file': vcf_file,
                'pass': 'PASS' if results.get('nb_error') == 0 else 'FAIL',
                '10_error_list': '\n'.join(results['error_list'])
            }
            report_data.update(results)
            reports.append("""  * {vcf_file}: {pass}
    - number of error: {nb_error}
    - match results: {ref_match}/{nb_variant}
    - first 10 errors: {10_error_list}
    - see report for detail: {assembly_check_text_report}
""".format(**report_data))
        return '\n'.join(reports)

    def _sample_check_report(self):
        reports = []
        for analysis_alias in self.eload_cfg.query('validation', 'sample_check', 'analysis'):
            results = self.eload_cfg.query('validation', 'sample_check', 'analysis', analysis_alias)
            report_data = {
                'analysis_alias': analysis_alias,
                'pass': 'FAIL' if results.get('difference_exists') else 'PASS',
                'in_VCF_not_in_metadata': ', '.join(results['in_VCF_not_in_metadata']),
                'in_metadata_not_in_VCF': ', '.join(results['in_metadata_not_in_VCF'])
            }
            reports.append("""  * {analysis_alias}: {pass}
    - Samples that appear in the VCF but not in the Metadata sheet:: {in_VCF_not_in_metadata}
    - Samples that appear in the Metadata sheet but not in the VCF file(s): {in_metadata_not_in_VCF}
""".format(**report_data))
        return '\n'.join(reports)

    def report(self):
        """Collect information from the config and write the report."""

        report_data = {
            'validation_date': self.eload_cfg.query('validation', 'validation_date'),
            'vcf_check': 'PASS' if self.eload_cfg.query('validation', 'vcf_check', 'pass') else 'FAIL',
            'assembly_check': 'PASS' if self.eload_cfg.query('validation', 'assembly_check', 'pass') else 'FAIL',
            'sample_check': 'PASS' if self.eload_cfg.query('validation', 'sample_check', 'pass') else 'FAIL',
            'vcf_check_report': self._vcf_check_report(),
            'assembly_check_report': self._assembly_check_report(),
            'sample_check_report': self._sample_check_report()
        }

        report = """Validation performed on {validation_date}
VCF check: {vcf_check}
Assembly check: {assembly_check}
Sample names check: {sample_check}
----------------------------------

VCF check:
{vcf_check_report}
----------------------------------

Assembly check:
{assembly_check_report}
----------------------------------

Sample names check:
{sample_check_report}
----------------------------------
"""
        print(report.format(**report_data))


