#!/usr/bin/env python
import csv
import os
import re
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import resolve_single_file_path, get_nextflow_config_flag


class EloadValidation(Eload):

    # eva-sub-cli tasks and their mapping to granular tasks
    sub_cli_tasks = {
        'vcf_check': ['vcf_check', 'evidence_type_check'],
        'assembly_check': ['assembly_check', 'fasta_check'],
        'metadata_check': ['metadata_check'],
        'sample_check': ['sample_check']
    }
    all_validation_tasks = list(sub_cli_tasks.keys()) + ['structural_variant_check', 'naming_convention_check']

    def validate(self, validation_tasks=None, set_as_valid=False):
        if not validation_tasks:
            validation_tasks = self.all_validation_tasks

        # (Re-)Initialise the config file output
        self.eload_cfg.set('validation', 'validation_date', value=self.now)
        self.eload_cfg.set('validation', 'valid', value={})
        for validation_task in validation_tasks:
            self.eload_cfg.set('validation', validation_task, value={})

        # All validation tasks are run via nextflow
        output_dir = self._run_validation_workflow(validation_tasks)
        self._collect_validation_workflow_results(output_dir, validation_tasks)
        shutil.rmtree(output_dir)

        if set_as_valid is True:
            for validation_task in validation_tasks:
                self.eload_cfg.set('validation', validation_task, 'forced', value=True)

        self.mark_valid_files_and_metadata()

    def mark_valid_files_and_metadata(self):
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
            if self.eload_cfg.query('submission', 'metadata_json'):
                self.eload_cfg.set('validation', 'valid', 'metadata_json',
                                   value=self.eload_cfg.query('submission', 'metadata_json'))
            elif self.eload_cfg.query('submission', 'metadata_spreadsheet'):
                self.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                                   value=self.eload_cfg.query('submission', 'metadata_spreadsheet'))

    def _get_vcf_files(self):
        vcf_files = []
        for analysis_alias in self.eload_cfg.query('submission', 'analyses'):
            files = self.eload_cfg.query('submission', 'analyses', analysis_alias, 'vcf_files')
            vcf_files.extend(files) if files else None
        return vcf_files

    def _get_valid_vcf_files_by_analysis(self):
        vcf_files = {}
        valid_analysis_dict = self.eload_cfg.query('validation', 'valid', 'analyses')
        if valid_analysis_dict:
            for analysis_alias in valid_analysis_dict:
                vcf_files[self._unique_alias(analysis_alias)] = valid_analysis_dict[analysis_alias]['vcf_files']
        return vcf_files

    def parse_sv_check_log(self, sv_check_log):
        with open(sv_check_log) as open_file:
            nb_sv = open_file.readline().split()
        if nb_sv:
            return int(nb_sv[0])
        else:
            return 0

    def _generate_csv_mappings(self):
        vcf_files_mapping_csv = os.path.join(self.eload_dir, 'validation_vcf_files_mapping.csv')
        with open(vcf_files_mapping_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['vcf', 'fasta', 'report', 'assembly_accession'])
            analyses = self.eload_cfg.query('submission', 'analyses')
            for analysis_alias, analysis_data in analyses.items():
                fasta = analysis_data['assembly_fasta']
                report = analysis_data['assembly_report']
                assembly = analysis_data['assembly_accession']
                if analysis_data['vcf_files']:
                    for vcf_file in analysis_data['vcf_files']:
                        writer.writerow([vcf_file, fasta, report, assembly])
                else:
                    self.warning(f"VCF files for analysis {analysis_alias} not found")
        return vcf_files_mapping_csv

    def _run_validation_workflow(self, validation_tasks):
        assert self.eload_cfg.query('submission', 'metadata_json'), 'Metadata json is not set in the config file, Cannot proceed with validation'
        metadata_json = self.eload_cfg.query('submission', 'metadata_json')
        assert os.path.isfile(metadata_json), f'Metadata json {metadata_json} does not exist. Cannot proceed with validation'
        output_dir = self.create_nextflow_temp_output_directory()
        vcf_files_mapping_csv = self._generate_csv_mappings()
        cfg['executable']['python']['script_path'] = os.path.dirname(os.path.dirname(__file__))
        validation_config = {
            'vcf_files_mapping': vcf_files_mapping_csv,
            'output_dir': output_dir,
            'metadata_json': metadata_json,
            'executable': cfg['executable'],
            'validation_tasks': validation_tasks
        }
        # run the validation
        validation_config_file = os.path.join(self.eload_dir, 'validation_config_file.yaml')
        with open(validation_config_file, 'w') as open_file:
            yaml.safe_dump(validation_config, open_file)
        validation_script = os.path.join(NEXTFLOW_DIR, 'validation.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow Validation process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], validation_script,
                    '-params-file', validation_config_file,
                    '-work-dir', output_dir,
                    get_nextflow_config_flag()
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
        if any(task in validation_tasks for task in self.sub_cli_tasks):
            self._collect_eva_sub_cli_results(output_dir)
        if 'structural_variant_check' in validation_tasks:
            self._collect_structural_variant_check_results(vcf_files, output_dir)
        if 'naming_convention_check' in validation_tasks:
            self._collect_naming_convention_check_results(vcf_files, output_dir)

    def _collect_structural_variant_check_results(self, vcf_files, output_dir):
        # detect output files for structural variant check
        for vcf_file in vcf_files:
            vcf_name, ext = os.path.splitext(os.path.basename(vcf_file))

            tmp_sv_check_log = resolve_single_file_path(
                os.path.join(output_dir, 'sv_check',  vcf_name + '_sv_check.log')
            )
            tmp_sv_check_sv_vcf = resolve_single_file_path(
                os.path.join(output_dir, 'sv_check', vcf_name + '_sv_list.vcf.gz')
            )
            # move the output files
            sv_check_log = self._move_file(
                tmp_sv_check_log,
                os.path.join(self._get_dir('sv_check'), vcf_name + '_sv_check.log')
            )
            sv_check_sv_vcf = self._move_file(
                tmp_sv_check_sv_vcf,
                os.path.join(self._get_dir('sv_check'), vcf_name + '_sv_list.vcf.gz')
            )

            if sv_check_log and sv_check_sv_vcf:
                nb_sv = self.parse_sv_check_log(sv_check_log)
                self.eload_cfg.set('validation', 'structural_variant_check', 'files', os.path.basename(vcf_file),
                                   value={'has_structural_variant': nb_sv > 0, 'number_sv': nb_sv})
        self.eload_cfg.set('validation', 'structural_variant_check', 'pass', value=True)

    def _collect_naming_convention_check_results(self, vcf_files, output_dir):
        naming_conventions = set()
        for vcf_file in vcf_files:
            vcf_name, ext = os.path.splitext(os.path.basename(vcf_file))
            tmp_nc_check_yml = resolve_single_file_path(
                os.path.join(output_dir, 'naming_convention_check',  vcf_name + '_naming_convention.yml')
            )
            # move the output files
            nc_check_yml = self._move_file(
                tmp_nc_check_yml,
                os.path.join(self._get_dir('naming_convention_check'), vcf_name + '_naming_convention.yml')
            )
            if nc_check_yml:
                with open(nc_check_yml) as open_yaml:
                    data = yaml.safe_load(open_yaml)
                self.eload_cfg.set('validation', 'naming_convention_check', 'files', os.path.basename(vcf_file),
                                   value=data[0])
                naming_conventions.add(data[0]['naming_convention'])
        if len(naming_conventions) == 1:
            self.eload_cfg.set('validation', 'naming_convention_check', 'naming_convention',
                               value=naming_conventions.pop())
        self.eload_cfg.set('validation', 'naming_convention_check', 'pass', value=True)

    def _collect_eva_sub_cli_results(self, output_dir):
        # Move the results to the validations folder
        results_path = resolve_single_file_path(os.path.join(output_dir, 'validation_results.yaml'))
        results_dest_path = os.path.join(self._get_dir('eva_sub_cli'), 'validation_results.yaml')
        self._move_file(results_path, results_dest_path)
        source_validation_dir_path = resolve_single_file_path(os.path.join(output_dir, 'validation_output'))
        dest_validation_dir_path = os.path.join(self._get_dir('eva_sub_cli'), 'validation_output')
        # move the whole validation_output directory
        if os.path.exists(dest_validation_dir_path):
            shutil.rmtree(dest_validation_dir_path)
        self._move_file(source_validation_dir_path, dest_validation_dir_path)

        self._update_config_with_cli_results(results_dest_path)
        report_txt = resolve_single_file_path(os.path.join(dest_validation_dir_path, 'report.txt'))
        report_html = resolve_single_file_path(os.path.join(dest_validation_dir_path, 'report.html'))
        self._update_cli_report_with_new_path(report_txt, output_dir, dest_validation_dir_path)
        self._update_cli_report_with_new_path(report_html, output_dir, dest_validation_dir_path)
        self._update_cli_report_with_new_path(results_dest_path, output_dir, dest_validation_dir_path)

    def _update_cli_report_with_new_path(self, report_file, nextflow_dir,  new_path):
        with open(report_file) as open_file:
            content = open_file.read()
        with open(report_file, 'w') as open_file:
            nextflow_validation_output = os.path.join(nextflow_dir, 'validation_output')
            tmp = re.sub(nextflow_validation_output, new_path, content)
            nextflow_validation_output = os.path.join(nextflow_dir, r'\w{2}', r'\w{30}', 'validation_output')
            open_file.write(re.sub(nextflow_validation_output, new_path, tmp))

    def _update_config_with_cli_results(self, results_dest_path):
        """Update ELOAD config with pass/fail values and aggregation type (required for ingestion) from eva-sub-cli
        results."""
        if os.path.exists(results_dest_path):
            with open(results_dest_path) as open_yaml:
                results = yaml.safe_load(open_yaml)
                for task, granular_tasks in self.sub_cli_tasks.items():
                    passed = all(results.get(t, {}).get('pass', False) for t in granular_tasks)
                    self.eload_cfg.set('validation', task, 'pass', value=passed)

                # Store evidence/aggregation type separately as it's required for ingestion
                evidence_check_dict = results.get('evidence_type_check', {})
                aggregation_check_dict = {}
                for alias, evidence in evidence_check_dict.items():
                    if alias not in ['pass', 'report_path']:
                        aggregation_check_dict[self._unique_alias(alias)] = self._evidence_type_to_aggregation(evidence['evidence_type'])
                self.eload_cfg.set('validation', 'aggregation_check', 'analyses', value=aggregation_check_dict)

    def _evidence_type_to_aggregation(self, s):
        if s == 'genotype':
            return 'none'
        if s == 'allele_frequency':
            return 'basic'
        return s

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

    def _naming_convention_check_report(self):
        vcf_files_2_naming_conv = self.eload_cfg.query('validation', 'naming_convention_check', 'files')
        reports = []
        if vcf_files_2_naming_conv:
            reports.append(
                f"  * Naming convention: "
                f"{self.eload_cfg.query('validation', 'naming_convention_check', 'naming_convention')}"
            )
            for vcf_file in vcf_files_2_naming_conv:
                reports.append(f"    * {vcf_file}: {vcf_files_2_naming_conv[vcf_file]['naming_convention']}")
                if not vcf_files_2_naming_conv[vcf_file]['naming_convention']:
                    reports.append(f"    * {vcf_file}: {vcf_files_2_naming_conv[vcf_file]['naming_convention_map']}")
        return '\n'.join(reports)

    def _eva_sub_cli_report(self):
        report_path = os.path.join(self._get_dir('eva_sub_cli'), 'validation_output', 'report.txt')
        if os.path.exists(report_path):
            with open(report_path) as open_report:
                return open_report.read()
        if self.eload_cfg.query('submission', 'metadata_json'):
            return f'Process failed, check logs'
        return f'Did not run'

    def report(self):
        """Collect information from the config and write the report."""
        report_data = {
            'validation_date': self.eload_cfg.query('validation', 'validation_date'),
            'vcf_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'vcf_check')),
            'assembly_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'assembly_check')),
            'metadata_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'metadata_check')),
            'sample_check': self._check_pass_or_fail(self.eload_cfg.query('validation', 'sample_check')),
            'structural_variant_check': self._check_pass_or_fail(self.eload_cfg.query('validation',
                                                                                      'structural_variant_check')),
            'naming_convention_check': self._check_pass_or_fail(self.eload_cfg.query('validation',
                                                                                      'naming_convention_check')),
            'structural_variant_check_report': self._structural_variant_check_report(),
            'naming_convention_check_report': self._naming_convention_check_report(),
            'eva_sub_cli_report': self._eva_sub_cli_report()
        }

        report = """Validation performed on {validation_date}
VCF checks: {vcf_check}
Assembly checks: {assembly_check}
Metadata check: {metadata_check}
Sample check: {sample_check}
Structural variant check: {structural_variant_check}
Naming convention check: {naming_convention_check}
----------------------------------

eva-sub-cli:
{eva_sub_cli_report}
----------------------------------

Structural variant check:
{structural_variant_check_report}

----------------------------------

Naming convention check:
{naming_convention_check_report}

----------------------------------
"""
        print(report.format(**report_data))
