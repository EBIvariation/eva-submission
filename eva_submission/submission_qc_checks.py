import glob
import os
from ftplib import FTP
from pathlib import Path

import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
from retry import retry

from eva_submission.eload_submission import Eload
from eva_submission.submission_config import EloadConfig

logger = logging_config.get_logger(__name__)


class EloadQC(Eload):
    def __init__(self, eload_number, config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        self.profile = cfg['maven']['environment']
        self.private_config_xml_file = cfg['maven']['settings_file']
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        self.path_to_data_dir = Path(cfg['projects_dir'], self.project_accession)
        self.taxonomy = self.eload_cfg.query('submission', 'taxonomy_id')
        self.analyses = self.eload_cfg.query('brokering', 'analyses')

    def check_if_study_appears_in_dev(self):
        dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/studies/{self.project_accession}/summary"
        json_response = self.get_result_from_webservice(dev_url)
        if self.check_if_study_present_in_response(json_response, 'id', self.project_accession):
            self._study_dev_check_result = "PASS"
        else:
            self._study_dev_check_result = "FAIL"

        return f"""
                pass: {self._study_dev_check_result}"""

    def check_if_study_appears_in_variant_browser(self, species_name):
        dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/list?species={species_name}"
        json_response = self.get_result_from_webservice(dev_url)
        if self.check_if_study_present_in_response(json_response, 'studyId', self.project_accession):
            return True
        else:
            return False

    def check_if_study_appears_in_dev_metadata(self):
        missing_assemblies = []
        for analysis_data in self.analyses.values():
            species_name = self.get_species_name(analysis_data['assembly_accession'])
            if not self.check_if_study_appears_in_variant_browser(species_name):
                missing_assemblies.append(f"{species_name}({analysis_data['assembly_accession']})")

        self._study_dev_metadata_check_result = "PASS" if not missing_assemblies else "FAIL"
        return f"""
                pass: {self._study_dev_metadata_check_result}
                missing assemblies: {missing_assemblies if missing_assemblies else None}"""

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def get_result_from_webservice(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def check_if_study_present_in_response(self, res, key, project_accession):
        if any(res) and 'response' in res and len(res['response']) > 0:
            for response in res['response']:
                if response['numTotalResults'] >= 1:
                    for result in response['result']:
                        if result[key] == project_accession:
                            return True
        return False

    def get_species_name(self, assembly):
        with get_metadata_connection_handle(self.profile, self.private_config_xml_file) as pg_conn:
            query = f"""select concat(t.taxonomy_code, '_',a.assembly_code) from evapro.taxonomy t 
                        join evapro.assembly a on a.taxonomy_id = t.taxonomy_id 
                        where t.taxonomy_id = {self.taxonomy} and assembly_accession='{assembly}'"""
            return get_all_results_for_query(pg_conn, query)[0][0]

    def get_browsable_files_for_study(self):
        with get_metadata_connection_handle(self.profile, self.private_config_xml_file) as pg_conn:
            query = f"select filename from evapro.browsable_file where project_accession='{self.project_accession}'"
            return [filename for filename, in get_all_results_for_query(pg_conn, query)]

    def check_all_browsable_files_are_available_in_ftp(self, vcf_files):
        try:
            files_in_ftp = self.get_files_from_ftp(self.project_accession)
        except Exception as e:
            logger.error(f"Error fetching files from ftp for study {self.study_accession}. Exception  {e}")
            self._ftp_check_result = "FAIL"
            return f"""
                Error: Error fetching files from ftp for study {self.project_accession}"""

        if not files_in_ftp:
            logger.error(f"No file found in ftp for study {self.study_accession}")
            self._ftp_check_result = "FAIL"
            return f"""
                Error: No files found in FTP for study {self.project_accession}"""

        missing_files = []

        for file in vcf_files:
            if file not in files_in_ftp:
                missing_files.append(file)
            if f'{file}.tbi' not in files_in_ftp:
                missing_files.append(f'{file}.tbi')
            if f'{file}.csi' not in files_in_ftp:
                missing_files.append(f'{file}.csi')

            # accessioned files will not be present for human taxonomy
            if self.taxonomy != 9606:
                accessioned_file = file.replace('.vcf.gz', '.accessioned.vcf.gz')
                if accessioned_file not in files_in_ftp:
                    missing_files.append(accessioned_file)
                if f'{accessioned_file}.tbi' not in files_in_ftp:
                    missing_files.append(f'{accessioned_file}.tbi')
                if f'{accessioned_file}.csi' not in files_in_ftp:
                    missing_files.append(f'{accessioned_file}.csi')

        self._ftp_check_result = "PASS" if not missing_files else "FAIL"
        return f"""
                pass: {self._ftp_check_result} 
                missing files: {missing_files if missing_files else None}"""

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def get_files_from_ftp(self, project_accession):
        ftp = FTP('ftp.ebi.ac.uk', timeout=600)
        ftp.login()
        ftp.cwd(f'pub/databases/eva/{project_accession}')
        return ftp.nlst()

    def check_if_job_completed_successfully(self, file_path):
        with open(file_path, 'r') as f:
            for line in f:
                if "Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched" in line or \
                        "Running job 'genotyped-vcf-job' with parameters" in line or \
                        "Running job 'aggregated-vcf-job' with parameters" in line:
                    job_status = ""
                if 'Job: [FlowJob: [name=genotyped-vcf-job]] completed' in line or \
                        'Job: [FlowJob: [name=aggregated-vcf-job]] completed' in line or \
                        'Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] completed' in line:
                    job_status = line.split(" ")[-1].replace("[", "").replace("]", "").strip()
            if job_status == 'COMPLETED':
                return True
            elif job_status == 'FAILED':
                return False
            else:
                logger.error(f'Could not determine status of variant load job in file {file_path}')

    def check_if_variants_were_skipped(self, file_path):
        with open(file_path, 'r') as f:
            for line in f:
                if "Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched" in line:
                    variants_skipped = None
                if 'lines in the original VCF were skipped' in line:
                    variants_skipped = line.strip().split(":")[-1].strip().split(" ")[0].strip()

            return variants_skipped

    def check_for_errors_in_case_of_job_failure(self, file_name):
        with open(file_name, 'r') as f:
            for line in f:
                if "Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched" in line or \
                        "Running job 'genotyped-vcf-job' with parameters" in line or \
                        "Running job 'aggregated-vcf-job' with parameters" in line:
                    job_name = None
                if 'Encountered an error executing step' in line:
                    job_name = line[line.index("Encountered an error executing step"): line.rindex("in job")] \
                        .strip().split(" ")[-1]
            return job_name

    def check_if_accessioning_completed_successfully(self, vcf_files):
        failed_files = {}
        for file in vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_data_dir}/00_logs/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if accessioning job completed successfullyy
                if not self.check_if_job_completed_successfully(accessioning_log_files[0]):
                    failed_job = self.check_for_errors_in_case_of_job_failure(accessioning_log_files[0])
                    failed_files[
                        file] = f"failed_job - {failed_job}" if failed_job else f"failed_job name could not be retrieved"
            else:
                failed_files[file] = f"error : No accessioning file found for {file}"

        self._accessioning_job_check_result = "PASS" if not failed_files else "FAIL"
        report = f"""
                pass: {self._accessioning_job_check_result}"""
        if failed_files:
            report += f"""
                failed_files:"""
            for file, value in failed_files.items():
                report += f"""
                    {file} - {value}"""

        return report

    def check_if_variant_load_completed_successfully(self, vcf_files):
        failed_files = {}
        for file in vcf_files:
            pipeline_log_files = glob.glob(f"{self.path_to_data_dir}/00_logs/pipeline.*{file}*.log")
            if pipeline_log_files:
                # check if variant load job completed successfully
                if not self.check_if_job_completed_successfully(pipeline_log_files[0]):
                    failed_job = self.check_for_errors_in_case_of_job_failure(pipeline_log_files[0])
                    failed_files[
                        file] = f"failed_job - {failed_job}" if failed_job else f"failed_job name could not be retrieved"
            else:
                failed_files[file] = f"error : No pipeline file found for {file}"

        self._variant_load_job_check_result = "PASS" if not failed_files else "FAIL"
        report = f"""
                pass: {self._variant_load_job_check_result}"""
        if failed_files:
            report += f"""
                failed_files:"""
            for file, value in failed_files.items():
                report += f"""
                    {file} - {value}"""

        return report

    def check_if_variants_were_skipped_while_accessioning(self, vcf_files):
        failed_files = {}
        for file in vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_data_dir}/00_logs/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if any variants were skippped while accessioning
                variants_skipped = self.check_if_variants_were_skipped(accessioning_log_files[0])
                if variants_skipped:
                    failed_files[file] = f"{variants_skipped} variants skipped"
            else:
                failed_files[file] = f"error : No accessioning file found for {file}"

        self._variants_skipped_accessioning_check_result = "PASS" if not failed_files else "FAIL"
        report = f"""
                pass: {self._variants_skipped_accessioning_check_result}"""
        if failed_files:
            report += f"""
                failed_files:"""
            for file, value in failed_files.items():
                report += f"""
                    {file} - {value}"""

        return report

    def check_if_browsable_files_entered_correctly_in_db(self, vcf_files):
        browsable_files_from_db = self.get_browsable_files_for_study()
        missing_files = set(vcf_files) - set(browsable_files_from_db)
        self._browsable_files_check_result = "PASS" if len(missing_files) == 0 else "FAIL"

        return f"""
            pass : {self._browsable_files_check_result}
            expected files: {vcf_files}
            missing files: {missing_files if missing_files else 'None'}"""

    def run_qc_checks_for_submission(self):
        """Collect information from different qc methods and write the report."""
        vcf_files = []
        for analysis_data in self.analyses.values():
            for v_files in analysis_data['vcf_files'].values():
                vcf_files.append(os.path.basename(v_files['output_vcf_file']))

        browsable_files_report = self.check_if_browsable_files_entered_correctly_in_db(vcf_files)

        # No accessioning check is required for human
        if self.taxonomy != 9606:
            accessioning_job_report = self.check_if_accessioning_completed_successfully(vcf_files)
            variants_skipped_report = self.check_if_variants_were_skipped_while_accessioning(vcf_files)

        variant_load_report = self.check_if_variant_load_completed_successfully(vcf_files)

        ftp_report = self.check_all_browsable_files_are_available_in_ftp(vcf_files)

        study_dev_report = self.check_if_study_appears_in_dev()

        study_dev_metadata_report = self.check_if_study_appears_in_dev_metadata()

        report = f"""
        QC Result Summary:
        ------------------
        Browsable files check: {self._browsable_files_check_result}
        Accessioning job check: {self._accessioning_job_check_result}
        Variants Skipped accessioning check: {self._variants_skipped_accessioning_check_result}
        Variant load check: {self._variant_load_job_check_result}
        FTP check: {self._ftp_check_result}
        Study dev check: {self._study_dev_check_result}
        Study dev metadata check: {self._study_dev_metadata_check_result}
        ----------------------------------

        Browsable files check:
        {browsable_files_report}
        ---------------------------------
        
        Accessioning job check:
        {accessioning_job_report}
        ----------------------------------
        
        Variants skipped check:
        {variants_skipped_report}
        ----------------------------------
        
        Variant load check:
        {variant_load_report}
        ----------------------------------
        
        FTP check:
        {ftp_report}
        ----------------------------------
        
        Study dev check:
        {study_dev_report}
        ----------------------------------

        Study dev metadata check:
        {study_dev_metadata_report}
        ----------------------------------
        """
        print(report)
