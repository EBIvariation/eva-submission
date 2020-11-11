#!/usr/bin/env python
import glob
import os
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission.eload_utils import retrieve_assembly_accession_from_ncbi, retrieve_species_names_from_tax_id, \
    get_genome_fasta_and_report
from eva_submission.samples_checker import compare_spreadsheet_and_vcf
from eva_submission.submission_config import EloadConfig
from eva_submission.submission_in_ftp import FtpDepositBox
from eva_submission.xlsreader import EVAXLSReader


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

    def _get_dir(self, key):
        return os.path.join(self.eload_dir, directory_structure[key])

    def _add_to_submission_config(self, key, value):
        if 'submission' in self.eload_cfg:
            self.eload_cfg['submission'][key] = value
        else:
            self.eload_cfg['submission'] = {key: value}


class EloadPrepation(Eload):

    def copy_from_ftp(self, ftp_box, username):
        box = FtpDepositBox(ftp_box, username)

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
        self._add_to_submission_config('metadata_spreadsheet', metadata_spreadsheets[0])

    def detect_submitted_vcf(self):
        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        uncompressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf'))
        compressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf.gz'))
        vcf_files = uncompressed_vcf + compressed_vcf
        if len(vcf_files) < 1:
            raise FileNotFoundError('Could not locate vcf file in in %s', vcf_dir)
        self._add_to_submission_config('vcf_files', vcf_files)

    def detect_metadata_attibutes(self):
        eva_metadata = EVAXLSReader(self.eload_cfg.query('submission', 'metadata_spreadsheet'))
        reference_gca = set()
        for analysis in eva_metadata.analysis:
            reference_txt = analysis.get('Reference')
            reference_gca.update(retrieve_assembly_accession_from_ncbi(reference_txt))

        if len(reference_gca) > 1:
            self.error('Multiple assemblies per project not currently supported: %s', ', '.join(reference_gca))
        elif reference_gca:
            self._add_to_submission_config('assembly_accession', reference_gca.pop())
        else:
            self.error('No genbank accession could be found for %s', reference_txt)

        taxonomy_id = eva_metadata.project.get('Tax ID')
        if taxonomy_id and (isinstance(taxonomy_id, int) or taxonomy_id.isdigit()):
            self._add_to_submission_config('taxonomy_id', int(taxonomy_id))
            scientific_name = retrieve_species_names_from_tax_id(taxonomy_id)
            self._add_to_submission_config('scientific_name', scientific_name)
        else:
            if taxonomy_id:
                self.error('Taxonomy id %s is invalid:', taxonomy_id)
            else:
                self.error('Taxonomy id is missing:')

    def find_genome(self):
        assembly_fasta_path, assembly_report_path = get_genome_fasta_and_report(
            self.eload_cfg.query('submission', 'scientific_name'),
            self.eload_cfg.query('submission', 'assembly_accession')
        )
        self._add_to_submission_config('assembly_fasta', assembly_fasta_path)
        self._add_to_submission_config('assembly_report', assembly_report_path)


class EloadValidation(Eload):

    def validate(self):
        validation_config = {
            'metadata_file': self.eload_cfg.query('submission', 'metadata_spreadsheet'),
            'vcf_files': self.eload_cfg.query('submission', 'vcf_files'),
            'reference_fasta': self.eload_cfg.query('submission', 'assembly_fasta'),
            'reference_report': self.eload_cfg.query('submission', 'assembly_report'),
            'output_dir': self._get_dir('validation'),
            'executable': cfg['executable']
        }

        # Check if the files are in the xls if not add them
        compare_spreadsheet_and_vcf(self.eload_cfg.query('submission', 'metadata_spreadsheet'), self._get_dir('vcf'))

        # run the validation
        validation_confg_file = os.path.join(self.eload_dir, 'validation_confg_file.yaml')
        with open(validation_confg_file, 'w') as open_file:
            yaml.safe_dump(validation_config, open_file)
        validation_script = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nextflow', 'validation.nf')
        try:
            command_utils.run_command_with_output(
                'Start Nextflow Validation process',
                cfg['executable']['nextflow'] + ' ' + validation_script + ' -params-file ' + validation_confg_file
            )
        except subprocess.CalledProcessError:
            self.error('Nextflow pipeline failed: results might not be complete')






