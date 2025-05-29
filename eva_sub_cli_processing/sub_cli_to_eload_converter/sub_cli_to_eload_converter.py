import json
import os
import shutil
from functools import cached_property

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_to_eload_converter.json_to_xlsx_converter import JsonToXlsxConverter
from eva_sub_cli_processing.sub_cli_utils import sub_ws_url_build, get_from_sub_ws
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import EloadConfig


class SubCLIToEloadConverter(EloadPreparation):

    def __init__(self, eload_number: int, config_object: EloadConfig = None, submission_id:str = None):
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
    def metadata_json_for_submission_id(self):
        metadata_json_data = self._submission_obj.get('metadataJson', {})
        if not metadata_json_data:
            raise ValueError(f"Metadata json retrieval: missing metadata_json field in the response for "
                             f"submission {self.submission_id}")
        return metadata_json_data

    @property
    def sub_cli_submission_dir_path(self):
        return os.path.join(cfg['ftp_dir'], 'eva-sub-cli', 'upload', self.submission_account_id, self.submission_id)

    def check_status(self):
        status = self._submission_obj.get('submission', {}).get('status')
        assert status == 'UPLOADED', f'Status for submission {self.submission_id} must be UPLOADED'

    def retrieve_vcf_files_from_sub_cli_ftp_dir(self):
        vcf_dir = self._get_dir('vcf')
        for root, dirs, files in os.walk(self.sub_cli_submission_dir_path):
            for name in files:
                file_path = os.path.join(root, name)
                if file_path.endswith('.vcf.gz') or file_path.endswith('.vcf'):
                    dest = os.path.join(vcf_dir, os.path.basename(file_path))
                    shutil.copyfile(file_path, dest)


    def download_metadata_json_and_convert_to_xlsx(self):
        metadata_dir = self._get_dir('metadata')
        metadata_json_file_path = os.path.join(metadata_dir, "metadata_json.json")
        metadata_xlsx_file_path = os.path.join(metadata_dir, "metadata_xlsx.xlsx")
        # download metadata json
        self._download_metadata_json_file_for_submission_id(metadata_json_file_path)
        # Store path to metadata json in the eload config
        self.eload_cfg.set('submission', 'metadata_json', value=metadata_json_file_path)
        # convert metadata json to metadata xlsx
        JsonToXlsxConverter(metadata_json_file_path, metadata_xlsx_file_path).convert_json_to_xlsx()

    def _download_metadata_json_file_for_submission_id(self, metadata_json_file_path):
        with open(metadata_json_file_path, "w", encoding="utf-8") as open_file:
            json.dump(self.metadata_json_for_submission_id, open_file, indent=4)