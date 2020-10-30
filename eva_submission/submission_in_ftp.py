import glob
import os
from datetime import datetime

import humanize
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.xlsreader import EVAXLSReader

logger = log_cfg.get_logger(__name__)


def inspect_all_users(ftp_box):
    deposit_boxes = glob.glob(os.path.join(cfg['ftp_dir'], ftp_box, '*'))
    for box in deposit_boxes:
        username = os.path.basename(box)
        box = FtpDepositBox(ftp_box, username)
        box.report()


class FtpDepositBox:

    def __init__(self, ftp_box, username):
        self.box = ftp_box
        self.username = username
        self._vcf_files = []
        self._metadata_files = []
        self._other_files = []
        self._explore()

    def _explore(self):
        for root, dirs, files in os.walk(self.deposit_box):
            for name in files:
                file_path = os.path.join(root, name)
                st = os.stat(file_path)
                if file_path.endswith('.vcf.gz') or file_path.endswith('.vcf'):
                    self._vcf_files.append((file_path, st.st_size, datetime.fromtimestamp(st.st_mtime)))
                elif file_path.endswith('.xlsx'):
                    self._metadata_files.append((file_path, st.st_size, datetime.fromtimestamp(st.st_mtime)))
                else:
                    self._other_files.append((file_path, st.st_size, datetime.fromtimestamp(st.st_mtime)))

    @property
    def deposit_box(self):
        return os.path.join(cfg['ftp_dir'], 'eva-box-%02d' % self.box, 'upload', self.username)

    @staticmethod
    def size_of(file_list):
        return sum([s for f, s, t in file_list])

    @staticmethod
    def last_modified_of(file_list):
        if file_list:
            return max([t for f, s, t in file_list])

    @property
    def size(self):
        return sum((
            self.size_of(self._vcf_files),
            self.size_of(self._metadata_files),
            self.size_of(self._other_files)
        ))

    @property
    def last_modified(self):
        return max([t for t in [
            self.last_modified_of(self._vcf_files),
            self.last_modified_of(self._metadata_files),
            self.last_modified_of(self._other_files)
        ] if t is not None])

    @property
    def vcf_files(self):
        return [f for f, _, _ in self._vcf_files]

    @property
    def metadata_files(self):
        return [f for f, _, _ in self._metadata_files]

    @property
    def other_files(self):
        return [f for f, _, _ in self._other_files]


    def report(self):
        report_params = {
            'ftp_box': self.deposit_box,
            'ftp_box_last_modified': self.last_modified,
            'ftp_box_size': humanize.naturalsize(self.size),
            'number_vcf': len(self._vcf_files),
            'vcf_last_modified': self.last_modified_of(self._vcf_files),
            'vcf_size': humanize.naturalsize(self.size_of(self._vcf_files)),
            'number_metadata': len(self._metadata_files),
            'metadata_last_modified': self.last_modified_of(self._metadata_files)
        }
        if self._metadata_files:
            reader = EVAXLSReader(self.metadata_files[0])
            report_params['project_title'] = reader.project_title
            report_params['number_analysis'] = len(reader.analysis)
            report_params['number_samples'] = len(reader.samples)

        report = """ftp box: {ftp_box}
last modified: {ftp_box_last_modified}
size: {ftp_box_size}
-----
number of vcf files: {number_vcf}
last modified: {vcf_last_modified}
size: {vcf_size}
-----
number of metadata spreadsheet: {number_metadata}
last modified: {metadata_last_modified}
Project title: {project_title}
Number of analysis: {number_analysis}
Number of sample: {number_samples}
"""
        print(report.format(**report_params))
