import gzip
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
        self.lts_archive_file = os.path.join(cfg['eloads_lts_dir'], f'{self.eload}.tar')

    def delete_submission(self, ftp_box, submitter, force_delete=False):
        # check if already present in LTS
        if os.path.exists(self.lts_archive_file) and not force_delete:
            raise Exception(
                f'File already exists in the LTS for the eload {self.eload_num}. LTS file: {self.lts_archive_file}')

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

    def is_compressed_or_index_file(self, file_name):
        return self.is_compressed(file_name) or file_name.endswith(".csi")

    def archive_eload(self):
        archive_dir = os.path.join(self.eload_dir, 'archive_dir')
        # delete if already exists
        shutil.rmtree(archive_dir, ignore_errors=True)
        os.makedirs(archive_dir, exist_ok=True)

        # copy relevant files to the archive_dir
        self.copy_eload_files(archive_dir)

        # gzip each file if they are not already compressed
        for root, _, files in os.walk(archive_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if not self.is_compressed_or_index_file(file):
                    gzip_path = f"{file_path}.gz"
                    with open(file_path, 'rb') as f_in, gzip.open(gzip_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    os.remove(file_path)

        # Create a tar archive of the entire archive_dir
        archive_tar_file = os.path.join(self.eload_dir, f'{self.eload}.tar')
        with tarfile.open(archive_tar_file, mode="w") as tar:
            for root, _, files in os.walk(archive_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=archive_dir)  # Avoid nesting archive_dir
                    tar.add(file_path, arcname=arcname)

        # copy to LTS
        try:
            shutil.copy(archive_tar_file, self.lts_archive_file)
        except Exception as e:
            print(f"Error copying archive to LTS: {e}")

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
        for file in Path(src_ena_dir).glob("*.csi"):
            if file.name.endswith(".vcf.gz.csi") or file.name.endswith(".vcf.csi"):
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
        for file in Path(real_accessioned_files_dir).glob("*.accessioned.vcf.gz*"):
            if file.name.endswith(".accessioned.vcf.gz") or file.name.endswith(".accessioned.vcf.gz.csi"):
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
