import glob
import os
from collections import defaultdict
from ftplib import FTP
from pathlib import Path

import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query
from requests import HTTPError
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
        self.path_to_logs_dir = os.path.join(self.path_to_data_dir, '00_logs')
        self.taxonomy = self.eload_cfg.query('submission', 'taxonomy_id')
        self.analyses = self.eload_cfg.query('brokering', 'analyses')
        self.job_launched_and_completed_text_map = {
            'accession': (
                {'Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched'},
                {'Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] completed'}
            ),
            'variant_load': (
                {'Job: [FlowJob: [name=genotyped-vcf-job]] launched',
                 'Job: [FlowJob: [name=aggregated-vcf-job]] launched'},
                {'Job: [FlowJob: [name=genotyped-vcf-job]] completed',
                 'Job: [FlowJob: [name=aggregated-vcf-job]] completed'}
            ),
            'acc_import': (
                {'Job: [SimpleJob: [name=accession-import-job]] launched'},
                {'Job: [SimpleJob: [name=accession-import-job]] completed'}
            ),
            'clustering': (
                {'Job: [SimpleJob: [name=STUDY_CLUSTERING_JOB]] launched'},
                {'Job: [SimpleJob: [name=STUDY_CLUSTERING_JOB]] completed'}
            ),
            'clustering_qc': (
                {'Job: [SimpleJob: [name=NEW_CLUSTERED_VARIANTS_QC_JOB]] launched'},
                {'Job: [SimpleJob: [name=NEW_CLUSTERED_VARIANTS_QC_JOB]] completed'}
            ),
            'vcf_extractor': (
                {'Job: [SimpleJob: [name=EXPORT_SUBMITTED_VARIANTS_JOB]] launched'},
                {'Job: [SimpleJob: [name=EXPORT_SUBMITTED_VARIANTS_JOB]] completed'}
            ),
            'remapping_ingestion': (
                {'Job: [SimpleJob: [name=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB]] launched'},
                {'Job: [SimpleJob: [name=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB]] completed'}
            ),
            'backpropagation': (
                {'Job: [SimpleJob: [name=BACK_PROPAGATE_NEW_RS_JOB]] launched'},
                {'Job: [SimpleJob: [name=BACK_PROPAGATE_NEW_RS_JOB]] completed'}
            )
        }

    def check_if_study_appears(self):
        url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/studies/{self.project_accession}/summary"
        try:
            json_response = self.get_result_from_webservice(url)
        except HTTPError as e:
            logger.error(str(e))
            json_response = {}
        if self.check_if_study_present_in_response(json_response, 'id'):
            self._study_check_result = "PASS"
        else:
            self._study_check_result = "FAIL"

        return f"""
                pass: {self._study_check_result}"""

    def check_if_study_appears_in_variant_browser(self, species_name):
        url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/list?species={species_name}"
        try:
            json_response = self.get_result_from_webservice(url)
        except HTTPError as e:
            logger.error(str(e))
            json_response = {}
        if self.check_if_study_present_in_response(json_response, 'studyId'):
            return True
        else:
            return False

    def check_if_study_appears_in_metadata(self):
        missing_assemblies = []
        for analysis_data in self.analyses.values():
            species_name = self.get_species_name(analysis_data['assembly_accession'])
            if not self.check_if_study_appears_in_variant_browser(species_name):
                missing_assemblies.append(f"{species_name}({analysis_data['assembly_accession']})")

        self._study_metadata_check_result = "PASS" if not missing_assemblies else "FAIL"
        return f"""
                pass: {self._study_metadata_check_result}
                missing assemblies: {missing_assemblies if missing_assemblies else None}"""

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def get_result_from_webservice(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def check_if_study_present_in_response(self, res, key):
        if any(res) and 'response' in res and len(res['response']) > 0:
            for response in res['response']:
                if response['numTotalResults'] >= 1:
                    for result in response['result']:
                        if result[key] == self.project_accession:
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
            logger.error(f"Error fetching files from ftp for study {self.project_accession}. Exception  {e}")
            self._ftp_check_result = "FAIL"
            return f"""
                Error: Error fetching files from ftp for study {self.project_accession}"""

        if not files_in_ftp:
            logger.error(f"No file found in ftp for study {self.project_accession}")
            self._ftp_check_result = "FAIL"
            return f"""
                Error: No files found in FTP for study {self.project_accession}"""

        missing_files = []

        for file in vcf_files:
            no_ext_file, _ = os.path.splitext(file)
            if file not in files_in_ftp:
                missing_files.append(file)
            if f'{file}.csi' not in files_in_ftp and f'{no_ext_file}.csi' not in files_in_ftp:
                missing_files.append(f'{file}.csi or {no_ext_file}.csi')

            # accessioned files will not be present for human taxonomy
            if self.taxonomy != 9606:
                accessioned_file = file.replace('.vcf.gz', '.accessioned.vcf.gz')
                no_ext_accessioned_file, _ = os.path.splitext(accessioned_file)
                if accessioned_file not in files_in_ftp:
                    missing_files.append(accessioned_file)
                if f'{accessioned_file}.csi' not in files_in_ftp and f'{no_ext_accessioned_file}.csi' not in files_in_ftp:
                    missing_files.append(f'{accessioned_file}.csi or {no_ext_accessioned_file}.csi')

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

    def check_if_job_completed_successfully(self, file_path, job_type):
        with open(file_path, 'r') as f:
            job_status = 'FAILED'
            job_launched_str, job_completed_str = self.job_launched_and_completed_text_map[job_type]
            for line in f:
                if any(str in line for str in job_launched_str):
                    job_status = ""
                if any(str in line for str in job_completed_str):
                    job_status = line.split(" ")[-1].replace("[", "").replace("]", "").strip()
            if job_status == 'COMPLETED':
                return True
            elif job_status == 'FAILED':
                return False
            else:
                logger.error(f'Could not determine status of {job_type} job in file {file_path}')
                return False

    def check_if_variants_were_skipped(self, file_path):
        with open(file_path, 'r') as f:
            variants_skipped = -1
            for line in f:
                if "Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched" in line:
                    variants_skipped = None
                if 'lines in the original VCF were skipped' in line:
                    variants_skipped = line.strip().split(":")[-1].strip().split(" ")[0].strip()

            return variants_skipped

    def get_failed_job_or_step_name(self, file_name):
        with open(file_name, 'r') as f:
            job_name = 'job name could not be retrieved'
            for line in f:
                if 'Encountered an error executing step' in line:
                    job_name = line[line.index("Encountered an error executing step"): line.rindex("in job")] \
                        .strip().split(" ")[-1]

            return job_name

    def check_if_accessioning_completed_successfully(self, vcf_files):
        failed_files = {}
        for file in vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_logs_dir}/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if accessioning job completed successfully
                if not self.check_if_job_completed_successfully(accessioning_log_files[0], 'accession'):
                    failed_files[
                        file] = f"failed job/step : {self.get_failed_job_or_step_name(accessioning_log_files[0])}"
            else:
                failed_files[file] = f"Accessioning Error : No accessioning file found for {file}"

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
        failed_files = defaultdict(dict)
        for file in vcf_files:
            variant_load_log_files = glob.glob(f"{self.path_to_logs_dir}/pipeline.*{file}*.log")
            acc_import_log_files = glob.glob(f"{self.path_to_logs_dir}/acc_import.*{file}*.log")

            variant_load_error = ""
            if variant_load_log_files:
                # check if variant load job completed successfully
                if not self.check_if_job_completed_successfully(variant_load_log_files[0], 'variant_load'):
                    variant_load_error += f"variant load failed job/step : {self.get_failed_job_or_step_name(variant_load_log_files[0])}"
                    variant_load_result = "FAIL"
                else:
                    variant_load_result = "PASS"
            else:
                variant_load_error += f"variant load error : No variant load log file found for {file}"
                variant_load_result = "FAIL"

            acc_import_error = ""
            if acc_import_log_files:
                # check if variant load job completed successfully
                if not self.check_if_job_completed_successfully(acc_import_log_files[0], 'acc_import'):
                    acc_import_error += f"accession import failed job/step : {self.get_failed_job_or_step_name(acc_import_log_files[0])}"
                    acc_import_result = "FAIL"
                else:
                    acc_import_result = "PASS"
            else:
                acc_import_error += f"accession import error : No acc import file found for {file}"
                acc_import_result = "FAIL"

            if variant_load_result == 'FAIL':
                failed_files[file]['variant_load'] = variant_load_error
            if acc_import_result == 'FAIL':
                failed_files[file]['acc_import'] = acc_import_error

        self._variant_load_job_check_result = "PASS"
        self._acc_import_job_check_result = "PASS"

        if failed_files:
            for file, errors in failed_files.items():
                if 'variant_load' in errors:
                    self._variant_load_job_check_result = "FAIL"
                if 'acc_import' in errors:
                    self._acc_import_job_check_result = "FAIL"

        report = f"""
                variant load result: {self._variant_load_job_check_result}
                accession import result: {self._acc_import_job_check_result}"""
        if failed_files:
            report += f"""
                    Failed Files:"""
            for file, error_txt in failed_files.items():
                variant_load_error = error_txt['variant_load'] if 'variant_load' in error_txt else ""
                acc_import_error = error_txt['acc_import'] if 'acc_import' in error_txt else ""
                report += f"""
                        {file}: 
                            {variant_load_error}
                            {acc_import_error}"""
        return report

    def check_if_variants_were_skipped_while_accessioning(self, vcf_files):
        failed_files = {}
        for file in vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_logs_dir}/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if any variants were skippped while accessioning
                variants_skipped = self.check_if_variants_were_skipped(accessioning_log_files[0])
                if variants_skipped:
                    if variants_skipped == -1:
                        failed_files[file] = f"could not retrieve skipped variants count"
                    else:
                        failed_files[file] = f"{variants_skipped} variants skipped"
            else:
                failed_files[file] = f"Accessioning Error : No accessioning file found for {file}"

        self._variants_skipped_accessioning_check_result = "PASS" if not failed_files else "PASS with Warning (Manual Check Required)"
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

    def clustering_check_report(self, target_assembly):
        clustering_log_file = glob.glob(f"{self.path_to_logs_dir}/{target_assembly}_clustering.log")
        clustering_qc_log_file = glob.glob(
            f"{self.path_to_logs_dir}/{target_assembly}_clustering_qc.log")

        clustering_error = ""
        if clustering_log_file:
            if not self.check_if_job_completed_successfully(clustering_log_file[0], 'clustering'):
                clustering_error += f"failed job/step : {self.get_failed_job_or_step_name(clustering_log_file[0])}"
                clustering_check_result = "FAIL"
            else:
                clustering_check_result = "PASS"
        else:
            clustering_error += f"Clustering Error : No clustering file found for {target_assembly}_clustering.log"
            clustering_check_result = "FAIL"

        clustering_qc_error = ""
        if clustering_qc_log_file:
            if not self.check_if_job_completed_successfully(clustering_qc_log_file[0], 'clustering_qc'):
                clustering_qc_error += f"failed job/step : {self.get_failed_job_or_step_name(clustering_qc_log_file[0])}"
                clustering_qc_check_result = "FAIL"
            else:
                clustering_qc_check_result = "PASS"
        else:
            clustering_qc_error += f"Clustering QC Error : No clustering qc file found for {target_assembly}_clustering_qc.log"
            clustering_qc_check_result = "FAIL"

        if clustering_check_result == 'FAIL' or clustering_qc_check_result == 'FAIL':
            self._clustering_check_result = 'FAIL'
        else:
            self._clustering_check_result = 'PASS'

        return f"""Clustering Job: {clustering_check_result}        
                        {clustering_error if clustering_check_result == 'FAIL' else ""}
                    Clustering QC Job: {clustering_qc_check_result}
                        {clustering_qc_error if clustering_qc_check_result == 'FAIL' else ""}
        """

    def remapping_check_report(self, target_assembly):
        asm_res = defaultdict(dict)
        for analysis_data in self.analyses.values():
            assembly_accession = analysis_data['assembly_accession']
            vcf_extractor_result = remapping_ingestion_result = 'SKIP'
            vcf_extractor_error = remapping_ingestion_error = ""
            if assembly_accession != target_assembly:
                vcf_extractor_log_file = glob.glob(
                    f"{self.path_to_logs_dir}/{assembly_accession}_vcf_extractor.log")
                remapped_ingestion_log_file = glob.glob(
                    f"{self.path_to_logs_dir}/{assembly_accession}_eva_remapped.vcf_ingestion.log")

                if vcf_extractor_log_file:
                    if not self.check_if_job_completed_successfully(vcf_extractor_log_file[0], 'vcf_extractor'):
                        vcf_extractor_error += f"failed job/step : {self.get_failed_job_or_step_name(vcf_extractor_log_file[0])}"
                        vcf_extractor_result = "FAIL"
                    else:
                        vcf_extractor_result = "PASS"
                else:
                    vcf_extractor_error += f"VCF Extractor Error: No vcf extractor file found for {assembly_accession}_vcf_extractor.log"
                    vcf_extractor_result = "FAIL"

                if remapped_ingestion_log_file:
                    if not self.check_if_job_completed_successfully(remapped_ingestion_log_file[0], 'remapping_ingestion'):
                        remapping_ingestion_error += f"failed job/step : {self.get_failed_job_or_step_name(remapped_ingestion_log_file[0])}"
                        remapping_ingestion_result = "FAIL"
                    else:
                        remapping_ingestion_result = "PASS"
                else:
                    remapping_ingestion_error += f"Remapping Ingestion Error: No remapping ingestion file found for {assembly_accession}_eva_remapped.vcf_ingestion.log"
                    remapping_ingestion_result = "FAIL"

            asm_res[assembly_accession]['vcf_extractor_result'] = vcf_extractor_result
            asm_res[assembly_accession]['vcf_extractor_error'] = vcf_extractor_error
            asm_res[assembly_accession]['remapping_ingestion_result'] = remapping_ingestion_result
            asm_res[assembly_accession]['remapping_ingestion_error'] = remapping_ingestion_error

        self._remapping_check_result = 'PASS'

        report = f"""remapping result of assemblies:"""
        for asm, res in asm_res.items():
            vcf_ext_res = res['vcf_extractor_result']
            vcf_ext_err = 'No Error' if res['vcf_extractor_error'] == "" else res['vcf_extractor_error']
            remap_ingest_res = res['remapping_ingestion_result']
            remap_ingest_err = 'No Error' if res['remapping_ingestion_error'] == "" else res['remapping_ingestion_error']
            if vcf_ext_res == 'FAIL' or remap_ingest_res == 'FAIL':
                self._remapping_check_result = 'FAIL'

            report += f"""
                        {asm}:
                            - vcf_extractor_result : {vcf_ext_res} - {vcf_ext_err}
                            - remapping_ingestion_result: {remap_ingest_res} - {remap_ingest_err}
                    """

        return report

    def backpropagation_check_report(self, target_assembly):
        asm_res = defaultdict(dict)
        for analysis_data in self.analyses.values():
            assembly_accession = analysis_data['assembly_accession']
            backpropagation_result = "SKIP"
            backpropagation_error = ""
            if assembly_accession != target_assembly:
                back_propagation_log_file = glob.glob(
                    f"{self.path_to_logs_dir}/{target_assembly}_backpropagate_to_{assembly_accession}.log")

                if back_propagation_log_file:
                    if not self.check_if_job_completed_successfully(back_propagation_log_file[0], 'backpropagation'):
                        backpropagation_error += f"failed job/step : {self.get_failed_job_or_step_name(back_propagation_log_file[0])}"
                        backpropagation_result = "FAIL"
                    else:
                        backpropagation_result = "PASS"
                else:
                    backpropagation_error += f"Backpropagation Error: No backpropagation file found for {target_assembly}_backpropagate_to_{assembly_accession}.log"
                    backpropagation_result = "FAIL"

            asm_res[assembly_accession]['result'] = backpropagation_result
            asm_res[assembly_accession]['error'] = backpropagation_error

        self._backpropagation_check_result = 'PASS'

        report = f"""backpropagation result of assemblies:"""
        for asm, result in asm_res.items():
            res = result['result']
            err = 'No Error' if result['error'] == '' else result['error']
            if res == 'FAIL':
                self._backpropagation_check_result = 'FAIL'
            report += f"""
                        {asm}: {res} - {err}"""

        return report

    def check_if_remapping_and_clustering_finished_successfully(self):
        target_assembly = self.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly')
        if not target_assembly:
            self._remapping_check_result = "FAIL"
            self._clustering_check_result = "FAIL"
            self._backpropagation_check_result = "FAIL"
            return f"""
                clustering check: {self._clustering_check_result}
                remapping check: {self._remapping_check_result}    
                backpropagation check: {self._backpropagation_check_result}
                Remapping and clustering have not run for this study (or eload configuration file is missing taxonomy)
                Note: This results might not be accurate for older studies. It is advisable to checks those manually
                """
        else:
            clustering_check_report = self.clustering_check_report(target_assembly)
            remapping_check_report = self.remapping_check_report(target_assembly)
            backpropagation_check_report = self.backpropagation_check_report(target_assembly)
            return f"""
                clustering check: {self._clustering_check_result}
                    {clustering_check_report}
                remapping check: {self._remapping_check_result}
                    {remapping_check_report}
                backpropagation check: {self._backpropagation_check_result}
                    {backpropagation_check_report}
                """

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
        else:
            self._accessioning_job_check_result = 'N/A - Human Taxonomy'
            self._variants_skipped_accessioning_check_result = 'N/A - Human Taxonomy'
            accessioning_job_report = f"""
            pass: {self._accessioning_job_check_result}"""
            variants_skipped_report = f"""
            pass: {self._variants_skipped_accessioning_check_result}"""

        variant_load_report = self.check_if_variant_load_completed_successfully(vcf_files)

        remapping_and_clustering_report = self.check_if_remapping_and_clustering_finished_successfully()

        ftp_report = self.check_all_browsable_files_are_available_in_ftp(vcf_files)

        study_report = self.check_if_study_appears()

        study_metadata_report = self.check_if_study_appears_in_metadata()

        report = f"""
        QC Result Summary:
        ------------------
        Browsable files check: {self._browsable_files_check_result}
        Accessioning job check: {self._accessioning_job_check_result}
        Variants Skipped accessioning check: {self._variants_skipped_accessioning_check_result}
        Variant load and Accession Import check:
            Variant load check: {self._variant_load_job_check_result}
            Accession Import check: {self._acc_import_job_check_result}
        Remapping and Clustering Check:
            Clustering check: {self._clustering_check_result} 
            Remapping check: {self._remapping_check_result}
            Back-propogation check: {self._backpropagation_check_result}
        FTP check: {self._ftp_check_result}
        Study check: {self._study_check_result}
        Study metadata check: {self._study_metadata_check_result}
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

        Remapping and Clustering check:
        {remapping_and_clustering_report}
        ----------------------------------
        
        FTP check:
        {ftp_report}
        ----------------------------------
        
        Study check:
        {study_report}
        ----------------------------------

        Study metadata check:
        {study_metadata_report}
        ----------------------------------
        """

        print(report)

        return report
