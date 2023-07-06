#!/usr/bin/env python
import copy
import csv
import os
import re
import shutil
import subprocess
import gzip

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from eva_vcf_merge.detect import detect_merge_type, MergeType
from eva_vcf_merge.merge import VCFMerger
from eva_vcf_merge.utils import validate_aliases

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import resolve_single_file_path, detect_vcf_aggregation
from eva_submission.samples_checker import compare_spreadsheet_and_vcf
from eva_submission.xlsx.xlsx_validation import EvaXlsxValidator


class EloadValidation(Eload):

    all_validation_tasks = ['metadata_check', 'assembly_check', 'aggregation_check', 'vcf_check', 'sample_check',
                            'structural_variant_check']

    def validate(self, validation_tasks=None, set_as_valid=False, merge_per_analysis=False):
        if not validation_tasks:
            validation_tasks = self.all_validation_tasks

        # (Re-)Initialise the config file output
        self.eload_cfg.set('validation', 'validation_date', value=self.now)
        self.eload_cfg.set('validation', 'valid', value={})
        for validation_task in validation_tasks:
            self.eload_cfg.set('validation', validation_task, value={})

        if 'metadata_check' in validation_tasks:
            self._validate_metadata_format()
        if 'sample_check' in validation_tasks:
            self._validate_sample_names()
        if 'aggregation_check' in validation_tasks:
            self._validate_genotype_aggregation()
        if set(validation_tasks).intersection(
                {'vcf_check', 'assembly_check', 'structural_variant_check'}
        ):
            output_dir = self._run_validation_workflow(validation_tasks)
            self._collect_validation_workflow_results(output_dir, validation_tasks)
            shutil.rmtree(output_dir)

        if set_as_valid is True:
            for validation_task in validation_tasks:
                self.eload_cfg.set('validation', validation_task, 'forced', value=True)

        self.mark_valid_files_and_metadata(merge_per_analysis)

    def mark_valid_files_and_metadata(self, merge_per_analysis):
        if all([
            self.eload_cfg.query('validation', validation_task, 'pass', ret_default=False) or
            self.eload_cfg.query('validation', validation_task, 'forced', ret_default=False)
            for validation_task in self.all_validation_tasks
        ]):
            for analysis_alias in self.eload_cfg.query('submission', 'analyses'):
                u_analysis_alias = self._unique_alias(analysis_alias)
                self.eload_cfg.set('validation', 'valid', 'analyses', u_analysis_alias,
                                   value=self.eload_cfg.query('submission', 'analyses', analysis_alias))
                self.eload_cfg.set(
                    'validation', 'valid', 'analyses', u_analysis_alias, 'vcf_files',
                    value=self.eload_cfg.query('submission', 'analyses', analysis_alias, 'vcf_files')
                )
            self.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                               value=self.eload_cfg.query('submission', 'metadata_spreadsheet'))
            self.detect_and_optionally_merge(merge_per_analysis)

    def _get_vcf_files(self):
        vcf_files = []
        for analysis_alias in self.eload_cfg.query('submission', 'analyses'):
            files = self.eload_cfg.query('submission', 'analyses', self._unique_alias(analysis_alias), 'vcf_files')
            vcf_files.extend(files) if files else None
        return vcf_files

    def _get_valid_vcf_files_by_analysis(self):
        vcf_files = {}
        valid_analysis_dict = self.eload_cfg.query('validation', 'valid', 'analyses')
        if valid_analysis_dict:
            for analysis_alias in valid_analysis_dict:
                vcf_files[self._unique_alias(analysis_alias)] = valid_analysis_dict[analysis_alias]['vcf_files']
        return vcf_files

    def _validate_metadata_format(self):
        validator = EvaXlsxValidator(self.eload_cfg['submission']['metadata_spreadsheet'])
        validator.validate()
        self.eload_cfg['validation']['metadata_check']['metadata_spreadsheet'] = self.eload_cfg['submission']['metadata_spreadsheet']
        self.eload_cfg['validation']['metadata_check']['errors'] = validator.error_list
        self.eload_cfg['validation']['metadata_check']['pass'] = len(validator.error_list) == 0

    def _validate_sample_names(self):
        overall_differences, results_per_analysis_alias = compare_spreadsheet_and_vcf(
            eva_files_sheet=self.eload_cfg['submission']['metadata_spreadsheet'],
            vcf_dir=self._get_dir('vcf')
        )
        for analysis_alias in results_per_analysis_alias:
            has_difference, diff_submitted_file_submission, diff_submission_submitted_file = results_per_analysis_alias[analysis_alias]
            analysis_alias = self._unique_alias(analysis_alias)
            self.eload_cfg.set('validation', 'sample_check', 'analysis', analysis_alias, value={
                'difference_exists': has_difference,
                'in_VCF_not_in_metadata': diff_submitted_file_submission,
                'in_metadata_not_in_VCF': diff_submission_submitted_file
            })
        self.eload_cfg.set('validation', 'sample_check', 'pass', value=not overall_differences)

    def _validate_genotype_aggregation(self):
        errors = []
        for analysis_alias in self.eload_cfg.query('submission', 'analyses'):
            aggregations = [
                detect_vcf_aggregation(vcf_file)
                for vcf_file in self.eload_cfg.query('submission', 'analyses', analysis_alias, 'vcf_files')
            ]
            analysis_alias = self._unique_alias(analysis_alias)
            if len(set(aggregations)) == 1 and None not in aggregations:
                aggregation = set(aggregations).pop()
                self.eload_cfg.set('validation', 'aggregation_check', 'analyses', str(analysis_alias), value=aggregation)
            elif None in aggregations:
                indices = [i for i, x in enumerate(aggregations) if x is None]
                errors.append(f'{analysis_alias}: VCF file aggregation could not be determied: ' + ', '.join([
                    self.eload_cfg.query('submission', 'analyses', analysis_alias, 'vcf_files')[i]
                    for i in indices
                ]))
            else:
                errors.append(f'{analysis_alias}: Multiple aggregation found: {",".join(set(aggregations))}')
        self.eload_cfg.set('validation', 'aggregation_check', 'errors', value=errors)
        self.eload_cfg.set('validation', 'aggregation_check', 'pass', value=len(errors) == 0)

    def detect_and_optionally_merge(self, merge_per_analysis):
        """Detects merge type for each analysis, but performs merge only when merge_per_analysis is True."""
        vcfs_by_analysis = self._get_valid_vcf_files_by_analysis()
        vcfs_to_horizontal_merge = {}
        vcfs_to_vertical_concat = {}
        for analysis_alias, vcf_files in vcfs_by_analysis.items():
            if len(vcf_files) < 2:
                continue
            analysis_alias = self._unique_alias(analysis_alias)
            merge_type = detect_merge_type(vcf_files)
            if merge_type:
                self.eload_cfg.set('validation', 'merge_type', analysis_alias, value=merge_type.value)
            if merge_type == MergeType.HORIZONTAL:
                vcfs_to_horizontal_merge[analysis_alias] = vcf_files
            elif merge_type == MergeType.VERTICAL:
                vcfs_to_vertical_concat[analysis_alias] = vcf_files
            else:
                self.error('Unsupported merge type!')

        if merge_per_analysis:
            if not validate_aliases(vcfs_by_analysis.keys()):
                self.error('Analysis aliases not valid as unique merged filenames, will not merge.')
                self.eload_cfg.set(
                    'validation', 'merge_errors',
                    value=['Analysis aliases not valid as unique merged filenames']
                )
                return
            merger = VCFMerger(
                bcftools_binary=cfg['executable']['bcftools'],
                bgzip_binary=cfg['executable']['bgzip'],
                nextflow_binary=cfg['executable']['nextflow'],
                nextflow_config=None,  # uses default Nextflow config
                output_dir=self._get_dir('merge')
            )
            merged_files = {}
            if vcfs_to_horizontal_merge:
                merged_files.update(merger.horizontal_merge(vcfs_to_horizontal_merge))
            if vcfs_to_vertical_concat:
                merged_files.update(merger.vertical_merge(vcfs_to_vertical_concat))
            # Overwrite valid vcf files in config for just these analyses
            for alias, merged_file in merged_files.items():
                self.eload_cfg.set('validation', 'valid', 'analyses', alias, 'vcf_files', value=[merged_file])

    def parse_assembly_check_log(self, assembly_check_log):
        error_list = []
        nb_error, nb_mismatch = 0, 0
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

    def parse_assembly_check_report(self, assembly_check_report):
        mismatch_list = []
        nb_mismatch = 0
        nb_error = 0
        error_list = []
        with open(assembly_check_report) as open_file:
            for line in open_file:
                if 'does not match the reference sequence' in line:
                    nb_mismatch += 1
                    if nb_mismatch < 11:
                        mismatch_list.append(line.strip())
                elif 'Multiple synonyms' in line:
                    nb_error += 1
                    if nb_error < 11:
                        error_list.append(line.strip())
        return mismatch_list, nb_mismatch, error_list, nb_error

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

    def parse_sv_check_log(self, sv_check_log):
        with open(sv_check_log) as open_file:
            nb_sv = int(open_file.readline().split()[0])
        return nb_sv

    def _generate_csv_mappings(self):
        vcf_files_mapping_csv = os.path.join(self.eload_dir, 'validation_vcf_files_mapping.csv')
        with open(vcf_files_mapping_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['vcf', 'fasta', 'report'])
            analyses = self.eload_cfg.query('submission', 'analyses')
            for analysis_alias, analysis_data in analyses.items():
                fasta = analysis_data['assembly_fasta']
                report = analysis_data['assembly_report']
                if analysis_data['vcf_files']:
                    for vcf_file in analysis_data['vcf_files']:
                        writer.writerow([vcf_file, fasta, report])
                else:
                    self.warning(f"VCF files for analysis {analysis_alias} not found")
        return vcf_files_mapping_csv

    def _run_validation_workflow(self, validation_tasks):
        output_dir = self.create_nextflow_temp_output_directory()
        vcf_files_mapping_csv = self._generate_csv_mappings()
        cfg['executable']['python']['script_path'] = os.path.dirname(os.path.dirname(__file__))
        validation_config = {
            'vcf_files_mapping': vcf_files_mapping_csv,
            'output_dir': output_dir,
            'executable': cfg['executable'],
            'validation_tasks': validation_tasks
        }
        # run the validation
        validation_confg_file = os.path.join(self.eload_dir, 'validation_confg_file.yaml')
        with open(validation_confg_file, 'w') as open_file:
            yaml.safe_dump(validation_config, open_file)
        validation_script = os.path.join(NEXTFLOW_DIR, 'validation.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow Validation process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
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

    def _collect_validation_workflow_results(self, output_dir, validation_tasks):
        # Collect information from the output and summarise in the config
        vcf_files = self._get_vcf_files()
        if 'vcf_check' in validation_tasks:
            self._collect_vcf_check_results(vcf_files, output_dir)
        if 'assembly_check' in validation_tasks:
            self._collect_assembly_check_results(vcf_files, output_dir)
        if 'structural_variant_check' in validation_tasks:
            self._collect_structural_variant_check_results(vcf_files, output_dir)

    def _collect_vcf_check_results(self, vcf_files, output_dir):
        total_error = 0
        # detect output files for vcf check
        for vcf_file in vcf_files:
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
                valid, error_list, error_count, warning_count = (False, ['Process failed'], 1, 0)
            total_error += error_count

            self.eload_cfg.set('validation', 'vcf_check', 'files', vcf_name, value={
                'error_list': error_list, 'nb_error': error_count, 'nb_warning': warning_count,
                'vcf_check_log': vcf_check_log, 'vcf_check_text_report': vcf_check_text_report,
                'vcf_check_db_report': vcf_check_db_report
            })
        self.eload_cfg.set('validation', 'vcf_check', 'pass', value=total_error == 0)

    def _collect_assembly_check_results(self, vcf_files, output_dir):
        # detect output files for assembly check
        total_error = 0
        for vcf_file in vcf_files:
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
                error_list_from_log, nb_error_from_log, match, total = \
                    self.parse_assembly_check_log(assembly_check_log)
                mismatch_list, nb_mismatch, error_list_from_report, nb_error_from_report = \
                    self.parse_assembly_check_report(assembly_check_text_report)
                nb_error = nb_error_from_log + nb_error_from_report
                error_list = error_list_from_log + error_list_from_report
            else:
                error_list, mismatch_list, nb_mismatch, nb_error, match, total = (['Process failed'], [], 0, 1, 0, 0)
            total_error += nb_error + nb_mismatch
            self.eload_cfg.set('validation', 'assembly_check', 'files', vcf_name, value={
                'error_list': error_list, 'mismatch_list': mismatch_list, 'nb_mismatch': nb_mismatch,
                'nb_error': nb_error, 'ref_match': match,
                'nb_variant': total, 'assembly_check_log': assembly_check_log,
                'assembly_check_valid_vcf': assembly_check_valid_vcf,
                'assembly_check_text_report': assembly_check_text_report
            })
        self.eload_cfg.set('validation', 'assembly_check', 'pass', value=total_error == 0)

    def _collect_structural_variant_check_results(self, vcf_files, output_dir):
        # detect output files for structural variant check
        for vcf_file in vcf_files:
            vcf_name = os.path.basename(vcf_file)

            tmp_sv_check_log = resolve_single_file_path(
                os.path.join(output_dir, 'sv_check',  vcf_name + '_sv_check.log')
            )
            tmp_sv_check_sv_vcf = resolve_single_file_path(
                os.path.join(output_dir, 'sv_check', vcf_name + '_sv_list.vcf')
            )

            # move the output files
            sv_check_log = self._move_file(
                tmp_sv_check_log,
                os.path.join(self._get_dir('sv_check'), vcf_name + '_sv_check.log')
            )
            sv_check_sv_vcf = self._move_file(
                tmp_sv_check_sv_vcf,
                os.path.join(self._get_dir('sv_check'), vcf_name + '_sv_list.vcf')
            )

            if sv_check_log and sv_check_sv_vcf:
                nb_sv = self.parse_sv_check_log(sv_check_log)
                self.eload_cfg.set('validation', 'structural_variant_check', 'files', os.path.basename(vcf_file),
                                   value={'has_structural_variant': nb_sv > 0, 'number_sv': nb_sv})
        self.eload_cfg.set('validation', 'structural_variant_check', 'pass', value=True)

    def _metadata_check_report(self):
        reports = []

        results = self.eload_cfg.query('validation', 'metadata_check', ret_default={})
        report_data = {
            'metadata_spreadsheet': results.get('metadata_spreadsheet'),
            'pass': 'PASS' if results.get('pass') else 'FAIL',
            'nb_error': len(results.get('errors', [])),
            'error_list': '\n'.join(results.get('errors', []))
        }
        reports.append("""  * {metadata_spreadsheet}: {pass}
    - number of error: {nb_error}
    - error messages: {error_list}
""".format(**report_data))
        return '\n'.join(reports)

    def _vcf_check_report(self):
        reports = []
        for vcf_file in self.eload_cfg.query('validation', 'vcf_check', 'files', ret_default=[]):
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
        for vcf_file in self.eload_cfg.query('validation', 'assembly_check', 'files', ret_default=[]):
            results = self.eload_cfg.query('validation', 'assembly_check', 'files', vcf_file)
            report_data = {
                'vcf_file': vcf_file,
                'pass': 'PASS' if results.get('nb_error') == 0 and results.get('nb_mismatch') == 0 else 'FAIL',
                '10_error_list': '\n'.join(results['error_list']),
                '10_mismatch_list': '\n'.join(results['mismatch_list']),
                'perc': (results.get('ref_match') or 0) / (results.get('nb_variant') or 1)
            }
            report_data.update(results)
            reports.append("""  * {vcf_file}: {pass}
    - number of error: {nb_error}
    - match results: {ref_match}/{nb_variant} ({perc:.1%})
    - first 10 errors: {10_error_list}
    - first 10 mismatches: {10_mismatch_list}
    - see report for detail: {assembly_check_text_report}
""".format(**report_data))
        return '\n'.join(reports)

    def _sample_check_report(self):
        reports = []
        for analysis_alias in self.eload_cfg.query('validation', 'sample_check', 'analysis', ret_default=[]):
            results = self.eload_cfg.query('validation', 'sample_check', 'analysis', analysis_alias)
            analysis_alias = self._unique_alias(analysis_alias)
            report_data = {
                'analysis_alias': analysis_alias,
                'pass': 'FAIL' if results.get('difference_exists') else 'PASS',
                'in_VCF_not_in_metadata': ', '.join(results['in_VCF_not_in_metadata']),
                'in_metadata_not_in_VCF': ', '.join(results['in_metadata_not_in_VCF'])
            }
            reports.append("""  * {analysis_alias}: {pass}
    - Samples that appear in the VCF but not in the Metadata sheet: {in_VCF_not_in_metadata}
    - Samples that appear in the Metadata sheet but not in the VCF file(s): {in_metadata_not_in_VCF}
""".format(**report_data))
        return '\n'.join(reports)

    def _vcf_merge_report(self):
        analysis_merge_dict = self.eload_cfg.query('validation', 'merge_type')
        if not analysis_merge_dict:
            return '  No mergeable VCFs\n'
        reports = ['  Merge types:']
        for analysis_alias, merge_type in analysis_merge_dict.items():
            analysis_alias = self._unique_alias(analysis_alias)
            reports.append(f'  * {analysis_alias}: {merge_type}')

        errors = self.eload_cfg.query('validation', 'merge_errors')
        if errors:
            reports.append('  Errors:')
            for error in errors:
                reports.append(f'  * {error}')
        return '\n'.join(reports)

    def _aggregation_report(self):
        aggregation_dict = self.eload_cfg.query('validation', 'aggregation_check')
        reports = []
        if aggregation_dict:
            for analysis_alias, aggregation in aggregation_dict.get('analyses', {}).items():
                analysis_alias = self._unique_alias(analysis_alias)
                reports.append(f"  * {analysis_alias}: {aggregation}")
            reports.append("  * Errors:")
            for error in aggregation_dict.get('errors', []):
                reports.append(f'    - {error}')
        else:
            reports.append('Not performed')
        return '\n'.join(reports)

    def _structural_variant_check_report(self):
        sv_dict = self.eload_cfg.query('validation', 'structural_variant_check', 'files')
        reports = []
        if sv_dict:
            for vcf_file, sv_check_status in sv_dict.items():
                if sv_check_status['has_structural_variant']:
                    reports.append(f'  * {vcf_file} has structural variants')
                else:
                    reports.append(f'  * {vcf_file} does not have structural variants')
        return '\n'.join(reports)

    def report(self):
        """Collect information from the config and write the report."""

        report_data = {
            'validation_date': self.eload_cfg.query('validation', 'validation_date'),
            'metadata_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'metadata_check')),
            'vcf_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'vcf_check')),
            'assembly_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'assembly_check')),
            'sample_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'sample_check')),
            'aggregation_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'aggregation_check')),
            'structural_variant_check': self._check_pass_or_fail(self.eload_cfg.query('validation',
                                                                                      'structural_variant_check')),
            'metadata_check_report': self._metadata_check_report(),
            'vcf_check_report': self._vcf_check_report(),
            'assembly_check_report': self._assembly_check_report(),
            'sample_check_report': self._sample_check_report(),
            'vcf_merge_report': self._vcf_merge_report(),
            'aggregation_report': self._aggregation_report(),
            'structural_variant_check_report': self._structural_variant_check_report()
        }

        report = """Validation performed on {validation_date}
Metadata check: {metadata_check}
VCF check: {vcf_check}
Assembly check: {assembly_check}
Sample names check: {sample_check}
Aggregation check: {aggregation_check}
Structural variant check: {structural_variant_check}
----------------------------------

Metadata check:
{metadata_check_report}
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

Aggregation:
{aggregation_report}

----------------------------------

VCF merge:
{vcf_merge_report}

----------------------------------

Structural variant check:
{structural_variant_check_report}

----------------------------------
"""
        print(report.format(**report_data))
