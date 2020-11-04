#!/usr/bin/env python
import glob
import os
import shutil

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.eload_config import EloadConfig
from eva_submission.submission_in_ftp import FtpDepositBox

logger = log_cfg.get_logger(__name__)

directory_structure = {
    'vcf': '10_submitted/vcf_files',
    'metadata': '10_submitted/metadata_file',
    'vcf_check': '13_validation/vcf_format',
    'assembly_check': '13_validation/assembly_check',
    'sample_check': '13_validation/sample_concordance',
    'biosamles': '18_brokering/biosamples',
    'ena': '18_brokering/ena',
    'scratch': '20_scratch'
}


class Eload:

    def __init__(self, eload_number: int):
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.eload_cfg = EloadConfig(os.path.join(self.eload_dir, '.' + self.eload + '_config.yml'))

        os.makedirs(self.eload_dir, exist_ok=True)
        for k in directory_structure:
            os.makedirs(self._get_dir(k), exist_ok=True)

    def _get_dir(self, key):
        return os.path.join(self.eload_dir, directory_structure[key])

    def copy_from_ftp(self, ftp_box, username):
        box = FtpDepositBox(ftp_box, username)

        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        for vcf_file in box.vcf_files:
            dest = os.path.join(vcf_dir, os.path.basename(vcf_file))
            shutil.copyfile(vcf_file, dest)

        if len(box.metadata_files) != 1:
            logger.warning('Found %s metadata file in the FTP. Will use the most recent one', len(box.metadata_files))
        metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
        dest = os.path.join(metadata_dir, os.path.basename(box.most_recent_metadata))
        shutil.copyfile(box.most_recent_metadata, dest)

        for other_file in box.other_files:
            logger.warning('File %s will not be treated', other_file)

    def detect_submitted_metadata(self):
        metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
        metadata_spreadsheets = glob.glob(os.path.join(metadata_dir, '*.xlsx'))
        if len(metadata_spreadsheets) != 1:
            raise ValueError('Found %s spreadsheet in %s', len(metadata_spreadsheets), metadata_dir)
        if 'submission' in self.eload_cfg:
            self.eload_cfg['submission']['metadata_spreadsheet'] = metadata_spreadsheets[0]
        else:
            self.eload_cfg['submission'] = {'metadata_spreadsheet': metadata_spreadsheets[0] }

    def detect_submitted_vcf(self):
        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        uncompressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf'))
        compressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf.gz'))
        vcf_files = uncompressed_vcf + compressed_vcf
        if len(vcf_files) < 1:
            raise FileNotFoundError('Could not locate vcf file in in %s', vcf_dir)
        if 'submission' in self.eload_cfg:
            self.eload_cfg['submission']['vcf_files'] = vcf_files
        else:
            self.eload_cfg['submission'] = {'vcf_files': vcf_files}

