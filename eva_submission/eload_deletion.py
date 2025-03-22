import os
import shutil
import tarfile
from pathlib import Path

from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_submission import Eload
from eva_submission.submission_in_ftp import deposit_box


class EloadDeletion(Eload):
    def __init__(self, eload_number):
        super().__init__(eload_number)
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        self.project_dir = os.path.join(cfg['projects_dir'], self.project_accession)

    def delete_submission(self, ftp_box, submitter):
        self.upgrade_to_new_version_if_needed()
        self.archive_eload()

        # delete
        ftp_dir = deposit_box(ftp_box, submitter)
        self.delete_ftp_dir(ftp_dir)
        self.delete_project_dir(self.project_dir)
        self.delete_eload_dir(self.eload_dir)

    def is_compressed(self, file_name):
        compressed_exts = (".gz", ".xz", ".bz2", ".zip", ".rar", ".7z")
        return file_name.endswith(compressed_exts)

    def archive_eload(self):
        archive_dir = os.path.join(self.eload_dir, 'archive_dir')
        os.makedirs(archive_dir, exist_ok=True)
        archive_file = os.path.join(self.eload_dir, f'{self.eload}.tar')
        # copy relevant files to the archive_dir
        self.copy_eload_files(archive_dir)

        # archive eload
        with tarfile.open(archive_file, "w:gz") as tar:
            for root, _, files in os.walk(archive_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=archive_dir)
                    if self.is_compressed(file):
                        tar.add(file_path, arcname=arcname, recursive=False, filter=lambda x: x)
                    else:
                        tar.add(file_path, arcname=arcname)

        # copy to lts
        shutil.copy(archive_file, cfg['eloads_lts_dir'])

    def copy_eload_files(self, archive_dir):
        # copy config file
        shutil.copy(self.config_path, archive_dir)

        # copy submission logs
        for file in Path(self.eload_dir).glob("*_submission.log"):
            shutil.copy(file, archive_dir)

        # copy metadata spreadsheet and vcf files along with index
        src_ena_dir = os.path.join(self.eload_dir, '18_brokering/ena')
        archive_ena_dir = os.path.join(archive_dir, '18_brokering/ena')
        os.makedirs(archive_ena_dir, exist_ok=True)
        shutil.copy(os.path.join(src_ena_dir, 'metadata_spreadsheet.xlsx'), archive_ena_dir)
        for file in Path(src_ena_dir).glob("*.vcf.gz"):
            shutil.copy(file, archive_ena_dir)
        for file in Path(src_ena_dir).glob("*.vcf.csi"):
            shutil.copy(file, archive_ena_dir)

        # copy 00_logs
        src_log_dir = os.path.join(self.eload_dir, '00_logs')
        real_src_log_dir = os.path.realpath(src_log_dir)
        archive_log_dir = os.path.join(archive_dir, '00_logs')
        shutil.copytree(real_src_log_dir, archive_log_dir)

        # copy accessioned files from 60_eva_public
        src_accessioned_files_dir = os.path.join(self.eload_dir, "60_eva_public")
        real_accessioned_files_dir = os.path.realpath(src_accessioned_files_dir)
        archive_accession_files_dir = os.path.join(archive_dir, "60_eva_public")
        os.makedirs(archive_accession_files_dir, exist_ok=True)
        for file in Path(real_accessioned_files_dir).glob("*.accessioned.vcf.gz"):
            shutil.copy(file, archive_accession_files_dir)

    def delete_ftp_dir(self, ftp_dir):
        self.info(f'Deleting FTP directory {ftp_dir}')
        if os.path.exists(ftp_dir):
            shutil.rmtree(ftp_dir)

    def delete_project_dir(self, project_dir):
        self.info(f'Deleting Project directory {project_dir}')
        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)

    def delete_eload_dir(self, eload_dir):
        self.info(f'Deleting Eload directory {eload_dir}')
        if os.path.exists(eload_dir):
            shutil.rmtree(eload_dir)
