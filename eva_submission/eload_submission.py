#!/usr/bin/env python
import glob
import os
import random
import shutil
import string
from datetime import datetime

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.eload_utils import get_hold_date_from_ena
from eva_submission.eload_utils import get_reference_fasta_and_report, resolve_accession_from_text
from eva_submission.submission_config import EloadConfig
from eva_submission.submission_in_ftp import FtpDepositBox
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

        for vcf_file in self.eload_cfg['brokering']['vcf_files']:
            original_vcf_file = self.eload_cfg['brokering']['vcf_files'][vcf_file]['original_vcf']
            file_row = file_to_row.get(os.path.basename(original_vcf_file), {})
            # Add the vcf file
            file_rows.append({
                'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
                'File Name': self.eload + '/' + os.path.basename(vcf_file),
                'File Type': 'vcf',
                'MD5': self.eload_cfg['brokering']['vcf_files'][vcf_file]['md5']
            })

            # Add the index file
            if self.eload_cfg['brokering']['vcf_files'][vcf_file]['index'].endswith('.csi'):
                file_type = 'csi'
            else:
                file_type = 'tabix'
            file_rows.append({
                'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
                'File Name': self.eload + '/' + os.path.basename(
                    self.eload_cfg['brokering']['vcf_files'][vcf_file]['index']),
                'File Type': file_type,
                'MD5': self.eload_cfg['brokering']['vcf_files'][vcf_file]['index_md5']
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

    def replace_values_in_metadata(self, taxid=None, reference_accession=None):
        """Find and Replace the value in the metadata spreadsheet with the one provided """
        # This might have been run before any detection occurred
        if not self.eload_cfg.query('submission', 'metadata_spreadsheet'):
            self.detect_submitted_metadata()
        input_spreadsheet = self.eload_cfg.query('submission', 'metadata_spreadsheet')
        reader = EvaXlsxReader(input_spreadsheet)

        # This will write the spreadsheet in place of the existing one
        eva_xls_writer = EvaXlsxWriter(input_spreadsheet)
        if taxid:
            project = reader.project
            project['Tax ID'] = taxid
            eva_xls_writer.set_project(project)
        if reference_accession:
            analysis_rows = []
            for analysis in reader.analysis:
                analysis['Reference'] = reference_accession
                analysis_rows.append(analysis)
            eva_xls_writer.set_analysis(analysis_rows)
        eva_xls_writer.save()

    def detect_all(self):
        self.detect_submitted_metadata()
        self.detect_metadata_attributes()
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
        self.eload_cfg.set('submission', 'vcf_files', value=vcf_files)

    def detect_metadata_attributes(self):
        eva_metadata = EvaXlsxReader(self.eload_cfg.query('submission', 'metadata_spreadsheet'))
        analysis_reference = {}
        for analysis in eva_metadata.analysis:
            reference_txt = analysis.get('Reference')
            assembly_accession = resolve_accession_from_text(reference_txt) if reference_txt else None
            if assembly_accession:
                analysis_reference[analysis.get('Analysis Alias')] = {'assembly_accession': assembly_accession,
                                                                      'vcf_files': []}
            else:
                self.error(f"Reference is missing for Analysis {analysis.get('Analysis Alias')}")

        for file in eva_metadata.files:
            if file.get("File Type") == 'vcf':
                file_full = os.path.join(self.eload_dir, directory_structure['vcf'], file.get("File Name"))
                analysis_alias = file.get("Analysis Alias")
                analysis_reference[analysis_alias]['vcf_files'].append(file_full)
        self.eload_cfg.set('submission', 'analyses', value=analysis_reference)

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
        scientific_name = self.eload_cfg.query('submission', 'scientific_name')
        analyses = self.eload_cfg.query('submission', 'analyses')
        if scientific_name:
            for analysis_alias in analyses:
                assembly_accession = self.eload_cfg.query('submission', 'analyses', analysis_alias, 'assembly_accession')
                assembly_fasta_path, assembly_report_path = get_reference_fasta_and_report(scientific_name, assembly_accession)
                if assembly_report_path:
                    self.eload_cfg.set('submission', 'analyses', analysis_alias, 'assembly_report', value=assembly_report_path)
                else:
                    self.warning(f'Assembly report was not set for {assembly_accession}')
                self.eload_cfg.set('submission', 'analyses', analysis_alias, 'assembly_fasta', value=assembly_fasta_path)
        else:
            self.error('No scientific name specified')
