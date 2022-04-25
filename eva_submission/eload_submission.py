#!/usr/bin/env python
import os
import random
import string
from datetime import datetime

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission import __version__
from eva_submission.config_migration import upgrade_version_0_1
from eva_submission.eload_utils import get_hold_date_from_ena
from eva_submission.submission_config import EloadConfig
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter

directory_structure = {
    'vcf': '10_submitted/vcf_files',
    'metadata': '10_submitted/metadata_file',
    'validation': '13_validation',
    'vcf_check': '13_validation/vcf_format',
    'assembly_check': '13_validation/assembly_check',
    'sample_check': '13_validation/sample_concordance',
    'merge': '14_merge',
    'biosamples': '18_brokering/biosamples',
    'ena': '18_brokering/ena',
    'scratch': '20_scratch'
}
eload_logging_files = set()


class Eload(AppLogger):
    def __init__(self, eload_number: int, config_object: EloadConfig = None):
        self.eload_num = eload_number
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.config_path = os.path.join(self.eload_dir, '.' + self.eload + '_config.yml')
        if config_object:
            self.eload_cfg = config_object
        else:
            self.eload_cfg = EloadConfig(self.config_path)

        os.makedirs(self.eload_dir, exist_ok=True)
        for k in directory_structure:
            os.makedirs(self._get_dir(k), exist_ok=True)
        self.create_log_file()

    @property
    def metadata_connection_handle(self):
        return get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file'])

    def create_nextflow_temp_output_directory(self, base=None):
        random_string = ''.join(random.choice(string.ascii_letters) for i in range(6))
        if base is None:
            output_dir = os.path.join(self.eload_dir, 'nextflow_output_' + random_string)
        else:
            output_dir = os.path.join(base, 'nextflow_output_' + random_string)
        os.makedirs(output_dir)
        return output_dir

    def _get_dir(self, key):
        return os.path.join(self.eload_dir, directory_structure[key])

    @cached_property
    def now(self):
        return datetime.now()

    def create_log_file(self):
        logfile_name = os.path.join(self.eload_dir, str(self.eload) + "_submission.log")
        if logfile_name not in eload_logging_files:
            log_cfg.add_file_handler(logfile_name)
            eload_logging_files.add(logfile_name)

    def upgrade_config_if_needed(self, analysis_alias=None):
        """
        Upgrades configs to the current version, making a backup first and using the provided analysis alias for all
        vcf files. Currently doesn't perform any other version upgrades.
        """
        if 'version' not in self.eload_cfg:
            self.debug(f'No version found in config, upgrading to version {__version__}.')
            self.eload_cfg.backup()
            upgrade_version_0_1(self.eload_cfg, analysis_alias)
        else:
            self.debug(f"Config is version {self.eload_cfg.query('version')}, not upgrading.")

    def update_config_with_hold_date(self, project_accession, project_alias=None):
        hold_date = get_hold_date_from_ena(project_accession, project_alias)
        self.eload_cfg.set('brokering', 'ena', 'hold_date', value=hold_date)

    def update_metadata_spreadsheet(self, input_spreadsheet, output_spreadsheet=None, existing_project=None):
        reader = EvaXlsxReader(input_spreadsheet)
        single_analysis_alias = None
        if len(reader.analysis) == 1:
            single_analysis_alias = reader.analysis[0].get('Analysis Alias')

        sample_rows = []
        for sample_row in reader.samples:
            if self.eload_cfg.query('brokering', 'Biosamples', 'Samples', sample_row.get('Sample Name')):
                sample_rows.append({
                    'row_num': sample_row.get('row_num'),
                    'Analysis Alias': sample_row.get('Analysis Alias') or single_analysis_alias,
                    'Sample ID': sample_row.get('Sample Name'),
                    'Sample Accession': self.eload_cfg['brokering']['Biosamples']['Samples'][sample_row.get('Sample Name')]
                })
            else:
                sample_rows.append(sample_row)

        file_rows = []
        analyses = self.eload_cfg['brokering']['analyses']
        for analysis in analyses:
            for vcf_file_name in analyses[analysis]['vcf_files']:
                vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                # Add the vcf file
                file_rows.append({
                    'Analysis Alias': analysis,
                    'File Name': self.eload + '/' + os.path.basename(vcf_file_name),
                    'File Type': 'vcf',
                    'MD5': vcf_file_info['md5']
                })

                # Add the index file
                if vcf_file_info['index'].endswith('.csi'):
                    file_type = 'csi'
                else:
                    file_type = 'tabix'
                file_rows.append({
                    'Analysis Alias': analysis,
                    'File Name': self.eload + '/' + os.path.basename(vcf_file_info['index']),
                    'File Type': file_type,
                    'MD5': vcf_file_info['index_md5']
                })

        project_row = reader.project
        if existing_project:
            project_row['Project Alias'] = existing_project

        if output_spreadsheet:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet, output_spreadsheet)
        else:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet)
        eva_xls_writer.set_project(project_row)
        eva_xls_writer.set_samples(sample_rows)
        eva_xls_writer.set_files(file_rows)
        eva_xls_writer.save()
        return output_spreadsheet

    @staticmethod
    def _check_pass_or_fail(check_dict):
        if check_dict and check_dict.get('forced'):
            return 'FORCED'
        if check_dict and check_dict.get('pass'):
            return 'PASS'
        return 'FAIL'
