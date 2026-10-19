import hashlib
import json
import os
import shutil
from functools import cached_property

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_utils import sub_ws_url_build, get_from_sub_ws, put_to_sub_ws
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import EloadConfig


def _md5(file_path):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as open_file:
        chunk = open_file.read(1024 * 1024)
        while chunk:
            md5.update(chunk)
            chunk = open_file.read(1024 * 1024)
    return md5.hexdigest()


class SubCLIToEloadConverter(EloadPreparation):

    def __init__(self, eload_number: int, submission_id:str, config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        self.submission_id = submission_id

    @cached_property
    def _submission_obj(self):
        submission_details_url = sub_ws_url_build("admin", "submission", self.submission_id)
        return get_from_sub_ws(submission_details_url)

    @property
    def submission_account_id(self):
        submission_account_id =  self._submission_obj.get('submission', {}).get('submissionAccount', {}).get('id')
        if not submission_account_id:
            raise ValueError(f"Missing: submission.submissionAccount.id field in the response for "
                             f"submission {self.submission_id}")
        return submission_account_id

    @property
    def metadata_json(self):
        metadata_json_data = self._submission_obj.get('metadataJson', {})
        if not metadata_json_data:
            raise ValueError(f"Metadata json retrieval: missing metadata_json field in the response for "
                             f"submission {self.submission_id}")
        return metadata_json_data

    @property
    def sub_cli_submission_dir_path(self) -> str:
        return str(os.path.join(cfg['ftp_dir'], 'eva-sub-cli', 'upload', self.submission_account_id, self.submission_id))

    def check_status(self):
        status = self._submission_obj.get('submission', {}).get('status')
        assert status == 'UPLOADED', \
            f'Status for submission {self.submission_id} must be UPLOADED. To prepare anyway, use --force_cli'

    def retrieve_vcf_files_from_sub_cli_ftp_dir(self):
        vcf_dir = self._get_dir('vcf')
        for root, dirs, files in os.walk(self.sub_cli_submission_dir_path):
            for name in files:
                file_path = os.path.join(root, name)
                if file_path.endswith('.vcf.gz') or file_path.endswith('.vcf'):
                    dest = os.path.join(vcf_dir, os.path.basename(file_path))
                    shutil.copyfile(file_path, dest)
        self.assemble_files_uploaded_in_parts(vcf_dir)
        self.check_vcf_files_md5(vcf_dir)

    def assemble_files_uploaded_in_parts(self, vcf_dir):
        """Rebuild the files that eva-sub-cli uploaded in parts, as described in the fileParts of the metadata."""
        for file_info in self.metadata_json.get('files', []):
            if file_info.get('fileParts'):
                self._assemble_file_from_parts(file_info, vcf_dir)

    def _assemble_file_from_parts(self, file_info, vcf_dir):
        file_name = os.path.basename(file_info.get('fileName'))
        part_paths = []
        for part in file_info.get('fileParts'):
            part_path = os.path.join(self.sub_cli_submission_dir_path, os.path.basename(part.get('fileName')))
            if not os.path.isfile(part_path):
                raise ValueError(f'Part {part.get("fileName")} of {file_name} was not found in '
                                 f'{self.sub_cli_submission_dir_path}')
            part_size = os.path.getsize(part_path)
            if part_size != part.get('fileSize'):
                raise ValueError(f'Part {part.get("fileName")} of {file_name} has a size of {part_size} bytes '
                                 f'instead of {part.get("fileSize")}')
            part_paths.append(part_path)

        dest = os.path.join(vcf_dir, file_name)
        with open(dest, 'wb') as output_file:
            for part_path in part_paths:
                with open(part_path, 'rb') as part_file:
                    shutil.copyfileobj(part_file, output_file)

        file_size = os.path.getsize(dest)
        if file_size != file_info.get('fileSize'):
            raise ValueError(f'{file_name} assembled from its parts has a size of {file_size} bytes instead of '
                             f'{file_info.get("fileSize")}')
        self.info(f'{file_name} assembled from {len(part_paths)} parts')

    def check_vcf_files_md5(self, vcf_dir):
        """Check that the md5 of each retrieved VCF file matches the md5 provided in the metadata."""
        for file_info in self.metadata_json.get('files', []):
            file_name = os.path.basename(file_info.get('fileName') or '')
            expected_md5 = file_info.get('md5')
            if not expected_md5 or not (file_name.endswith('.vcf.gz') or file_name.endswith('.vcf')):
                self.warning(f'{file_name} does not have an md5 or is not a vcf file')
                continue
            file_path = os.path.join(vcf_dir, file_name)
            if not os.path.isfile(file_path):
                raise ValueError(f'{file_name} described in the metadata was not found in {vcf_dir}')
            file_md5 = _md5(file_path)
            if file_md5 != expected_md5:
                raise ValueError(f'{file_name} has an md5 of {file_md5} instead of {expected_md5} provided in the '
                                 f'metadata')


    def download_metadata_json_and_store(self):
        metadata_dir = self._get_dir('metadata')
        metadata_json_file_path = os.path.join(metadata_dir, "metadata_json.json")
        # download metadata json
        self._download_metadata_json_file(metadata_json_file_path)
        # Store path to metadata json in the eload config
        self.eload_cfg.set('submission', 'metadata_json', value=metadata_json_file_path)


    def _download_metadata_json_file(self, metadata_json_file_path):
        with open(metadata_json_file_path, "w", encoding="utf-8") as open_file:
            json.dump(self.metadata_json, open_file, indent=4)

    def add_submission_id_to_config(self, source="eva-sub-cli"):
        json_data = {
            "eload": self.eload_num,
            "submissionId": self.submission_id,
            "source": source
        }
        put_to_sub_ws(sub_ws_url_build("admin", "submission", "eload", "submissionId"), json_data=json_data)
        self.eload_cfg.set('submission', 'submission_id', value=self.submission_id)
