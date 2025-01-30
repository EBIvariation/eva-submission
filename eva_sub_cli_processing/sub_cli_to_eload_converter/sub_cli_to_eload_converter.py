import json
import os
import shutil

import requests
from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_to_eload_converter.json_to_xlsx_converter import JsonToXlsxConverter
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.eload_submission import directory_structure


class SubCLIToEloadConverter(EloadPreparation):

    def retrieve_vcf_files_from_sub_cli_ftp_dir(self, submission_account_id, submission_id):
        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])

        sub_cli_submission_dir = self.get_sub_cli_submission_dir_path(submission_account_id, submission_id)
        for root, dirs, files in os.walk(sub_cli_submission_dir):
            for name in files:
                file_path = os.path.join(root, name)
                if file_path.endswith('.vcf.gz') or file_path.endswith('.vcf'):
                    dest = os.path.join(vcf_dir, os.path.basename(file_path))
                    shutil.copyfile(file_path, dest)

    def get_sub_cli_submission_dir_path(self, submission_account_id, submission_id):
        return os.path.join(cfg['ftp_dir'], 'eva-sub-cli', 'upload', submission_account_id, submission_id)

    def download_metadata_json_and_convert_to_xlsx(self, submission_id):
        metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
        metadata_json_file_path = os.path.join(metadata_dir, "metadata_json.json")
        metadata_xlsx_file_path = os.path.join(metadata_dir, "metadata_xlsx.xlsx")
        # download metadata json
        self.download_metadata_json(submission_id, metadata_json_file_path)
        # convert metadata json to metadata xlsx
        JsonToXlsxConverter().convert_json_to_xlsx(metadata_json_file_path, metadata_xlsx_file_path)

    def download_metadata_json(self, submission_id, metadata_json_file_path):
        url = f"{cfg['submissions']['webservice']['url']}/admin/submission/{submission_id}"
        username = cfg['submissions']['webservice']['admin_username']
        password = cfg['submissions']['webservice']['admin_password']
        response = requests.get(url, auth=(username, password))
        if response.status_code == 200:
            try:
                response_data = response.json()
                metadata_json_data = response_data['metadataJson']
                if metadata_json_data:
                    with open(metadata_json_file_path, "w", encoding="utf-8") as file:
                        json.dump(metadata_json_data, file, indent=4)

                    self.info(f"Metadata JSON file downloaded successfully: {metadata_json_file_path}")
                else:
                    raise ValueError("Metadata json file download: missing metadata_json field in the response")
            except Exception as e:
                raise Exception(f"Metadata json file download: Error processing response from the server: {e}")
        else:
            raise Exception(f"Failed to download metadata json file for submission id {submission_id}. "
                            f"Status code: {response.status_code}")
