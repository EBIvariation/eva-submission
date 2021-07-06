#!/usr/bin/env python
import os
import random
import string
from copy import deepcopy
from datetime import datetime

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission import __version__
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
    'biosamples': '18_brokering/biosamples',
    'ena': '18_brokering/ena',
    'scratch': '20_scratch'
}


class Eload(AppLogger):
    def __init__(self, eload_number: int):
        self.eload_num = eload_number
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.eload_cfg = EloadConfig(os.path.join(self.eload_dir, '.' + self.eload + '_config.yml'))

        os.makedirs(self.eload_dir, exist_ok=True)
        for k in directory_structure:
            os.makedirs(self._get_dir(k), exist_ok=True)

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

    def upgrade_config_if_needed(self, analysis_alias=None):
        """
        Upgrades unversioned configs (i.e. pre-1.0) to the current version, making a backup first and using the
        provided analysis alias for all vcf files. Currently doesn't perform any other version upgrades.
        """
        if 'version' not in self.eload_cfg:
            self.info(f'No version found in config, upgrading to version {__version__}.')
            self.eload_cfg.backup()

            if 'submission' not in self.eload_cfg:
                self.error('Need submission config section to upgrade')
                self.error('Try running prepare_submission or prepare_backlog_study to build a config from scratch.')
                raise ValueError('Need submission config section to upgrade')

            # Note: if we're converting an old config, there's only one analysis
            if not analysis_alias:
                analysis_alias = self._get_analysis_alias_from_metadata()
            analysis_data = {
                'assembly_accession': self.eload_cfg.pop('submission', 'assembly_accession'),
                'assembly_fasta': self.eload_cfg.pop('submission', 'assembly_fasta'),
                'assembly_report': self.eload_cfg.pop('submission', 'assembly_report'),
                'vcf_files': self.eload_cfg.pop('submission', 'vcf_files')
            }
            analysis_dict = {analysis_alias: analysis_data}
            self.eload_cfg.set('submission', 'analyses', value=analysis_dict)

            if 'validation' in self.eload_cfg:
                self.eload_cfg.pop('validation', 'valid', 'vcf_files')
                self.eload_cfg.set('validation', 'valid', 'analyses', value=analysis_dict)

            if 'brokering' in self.eload_cfg:
                brokering_vcfs = {
                    vcf_file: index_dict
                    for vcf_file, index_dict in self.eload_cfg.pop('brokering', 'vcf_files').items()
                }
                brokering_analyses = deepcopy(analysis_dict)
                brokering_analyses[analysis_alias]['vcf_files'] = brokering_vcfs
                self.eload_cfg.set('brokering', 'analyses', value=brokering_analyses)
                analysis_accession = self.eload_cfg.pop('brokering', 'ena', 'ANALYSIS')
                self.eload_cfg.set('brokering', 'ena', 'ANALYSIS', analysis_alias, value=analysis_accession)

            # Set version once we've successfully upgraded
            self.eload_cfg.set('version', value=__version__)
        else:
            self.info(f"Config is version {self.eload_cfg.query('version')}, not upgrading.")

    def _get_analysis_alias_from_metadata(self):
        """
        Returns analysis alias only if we find a metadata spreadsheet and it has exactly one analysis.
        Otherwise provides an error message and raise an error.
        """
        metadata_spreadsheet = self.eload_cfg.query('validation', 'valid', 'metadata_spreadsheet')
        if metadata_spreadsheet:
            reader = EvaXlsxReader(metadata_spreadsheet)
            if len(reader.analysis) == 1:
                return reader.analysis[0].get('Analysis Alias')

            if len(reader.analysis) > 1:
                self.error("Can't assign analysis alias: multiple analyses found in metadata!")
            else:
                self.error("Can't assign analysis alias: no analyses found in metadata!")
        else:
            self.error("Can't assign analysis alias: no metadata found!")
        self.error("Try running upgrade_config and passing an analysis alias explicitly.")
        raise ValueError("Can't find an analysis alias for config upgrade.")

    def update_config_with_hold_date(self, project_accession, project_alias=None):
        hold_date = get_hold_date_from_ena(project_accession, project_alias)
        self.eload_cfg.set('brokering', 'ena', 'hold_date', value=hold_date)

    def update_metadata_from_config(self, input_spreadsheet, output_spreadsheet=None):
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
        file_to_row = {}
        for file_row in reader.files:
            file_to_row[file_row['File Name']] = file_row

        analyses = self.eload_cfg['brokering']['analyses']
        for analysis in analyses:
            for vcf_file_name in analyses[analysis]['vcf_files']:
                vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                original_vcf_file = vcf_file_info['original_vcf']
                file_row = file_to_row.get(os.path.basename(original_vcf_file), {})
                # Add the vcf file
                file_rows.append({
                    'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
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
                    'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
                    'File Name': self.eload + '/' + os.path.basename(vcf_file_info['index']),
                    'File Type': file_type,
                    'MD5': vcf_file_info['index_md5']
                })
        if output_spreadsheet:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet, output_spreadsheet)
        else:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet)
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
