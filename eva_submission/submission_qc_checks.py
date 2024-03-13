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
            'load_vcf': (
                {'Job: [FlowJob: [name=load-vcf-job]] launched'},
                {'Job: [FlowJob: [name=load-vcf-job]] completed'}
            ),
            'annotate_variants': (
                {'Job: [FlowJob: [name=annotate-variants-job]] launched'},
                {'Job: [FlowJob: [name=annotate-variants-job]] completed'}
            ),
            'calculate_statistics': (
                {'Job: [FlowJob: [name=calculate-statistics-job]] launched'},
                {'Job: [FlowJob: [name=calculate-statistics-job]] completed'}
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

    def _did_job_complete_successfully_from_log(self, file_path, job_type):
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
                if not self._did_job_complete_successfully_from_log(accessioning_log_files[0], 'accession'):
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
        for file_name in vcf_files:
            self._find_log_and_check_job(
                file_name, f"pipeline.*{file_name}*.log", "variant_load", failed_files
            )
            self._find_log_and_check_job(
                file_name, f"load_variants.*{file_name}*.log", "load_vcf", failed_files
            )
            self._find_log_and_check_job(
                file_name, f"acc_import.*{file_name}*.log", "acc_import", failed_files
            )
        self._load_vcf_job_check_result = "PASS"
        self._acc_import_job_check_result = "PASS"
        if failed_files:
            for file_name in list(failed_files):
                errors = failed_files[file_name]
                if 'load_vcf' in errors and 'variant_load' in errors:
                    self._load_vcf_job_check_result = "FAIL"
                elif 'load_vcf' in errors and 'variant_load' not in errors:
                    # We can remove the load_vcf error because it is covered by variant_load
                    errors.pop('load_vcf')
                if 'acc_import' in errors:
                    self._acc_import_job_check_result = "FAIL"
                if not errors:
                    # If there are no more error we can remove the file completely
                    failed_files.pop(file_name)

        failed_analysis = defaultdict(dict)
        analysis_to_file_names = {}
        for analysis_alias, analysis_accession in self.eload_cfg.query('brokering', 'ena', 'ANALYSIS').items():
            # Find the files associated with this analysis
            analysis_to_file_names[analysis_accession] = [
                os.path.basename(f) for f in self.analyses.get(analysis_alias).get('vcf_files')
            ]
            # annotation only happens if a VEP cache can be found
            assembly_accession = self.eload_cfg.query('brokering', 'analyses', analysis_alias, 'assembly_accession')
            if self.eload_cfg.query('ingestion', 'vep', assembly_accession, 'cache_version') == None:
                self._find_log_and_check_job(
                    analysis_accession, f"annotation.*{analysis_accession}*.log", "annotate_variants", failed_analysis
                )
            # Statistics is only run if the aggregation is set to none
            if self.eload_cfg.query('ingestion', 'aggregation', analysis_accession, ret_default='none') == 'none':
                self._find_log_and_check_job(
                    analysis_accession, f"statistics.*{analysis_accession}*.log", "calculate_statistics", failed_analysis
                )

        self._annotation_job_check_result = "PASS"
        self._statistics_job_check_result = "PASS"
        if failed_analysis:
            for analysis_accession in list(failed_analysis):
                errors = failed_analysis[analysis_accession]
                # Check that the variant_load step didn't run the annotation and calculate statistics
                variant_load_error = any(
                    'variant_load' in failed_files.get(f, {}) for f in analysis_to_file_names[analysis_accession]
                )
                if 'annotate_variants' in errors and variant_load_error:
                    self._annotation_job_check_result = "FAIL"
                elif 'annotate_variants' in errors and not variant_load_error:
                    # We can remove the annotate_variants error because it is covered by variant_load
                    errors.pop('annotate_variants')
                if 'calculate_statistics' in errors and variant_load_error:
                    self._statistics_job_check_result = "FAIL"
                elif 'calculate_statistics' in errors and not variant_load_error:
                    # We can remove the calculate_statistics error because it is covered by variant_load
                    errors.pop('calculate_statistics')
                if not errors:
                    # If there are no more error we can remove the analysis completely
                    failed_analysis.pop(analysis_accession)

        report = f"""
                vcf load result: {self._load_vcf_job_check_result}
                annotation result: {self._annotation_job_check_result}
                statistics result: {self._statistics_job_check_result}
                accession import result: {self._acc_import_job_check_result}"""
        if failed_files:
            # For the report the variant_load does not needs to be reported because any new run will be done
            # with the new load_vcf method. Remove the variant_load from the failed files
            for file_name in failed_files:
                failed_files[file_name].pop('variant_load', None)
            report += f"""
                    Failed Files:"""
            for file_name, error_txt in failed_files.items():
                report += f"""
                        {file_name}: 
                            {error_txt.get("load_vcf", "")}
                            {error_txt.get("acc_import", "")}"""
        if failed_analysis:
            report += f"""
                    Failed Analysis:"""
            for analysis_accession, error_txt in failed_analysis.items():
                report += f"""
                        {analysis_accession}: 
                            {error_txt.get('annotate_variants', "")}
                            {error_txt.get('calculate_statistics', "")}"""
        return report

    def _find_log_and_check_job(self, search_unit, log_file_pattern, job_type, failure_dict=None):
        log_files = glob.glob(os.path.join(self.path_to_logs_dir, log_file_pattern))
        report_text = ""
        if log_files:
            # check if job completed successfully
            if not self._did_job_complete_successfully_from_log(log_files[0], job_type):
                report_text += f"{job_type} failed job/step : {self.get_failed_job_or_step_name(log_files[0])}"
                job_passed = False
            else:
                job_passed = True
        else:
            report_text += f"{job_type} error : No {job_type} log file found for {search_unit}"
            job_passed = False
        if not job_passed and failure_dict is not None:
            failure_dict[search_unit][job_type] = report_text
        return job_passed, report_text

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
        clustering_check_pass, clustering_error = self._find_log_and_check_job(
            target_assembly, f'{target_assembly}_clustering.log', 'clustering'
        )
        clustering_qc_check_pass, clustering_qc_error = self._find_log_and_check_job(
            target_assembly, f'{target_assembly}_clustering_qc.log', 'clustering_qc'
        )

        if clustering_check_pass and clustering_qc_check_pass:
            self._clustering_check_result = 'PASS'
        else:
            self._clustering_check_result = 'FAIL'

        return f"""Clustering Job: {'PASS' if clustering_check_pass else "FAIL"}        
                        {clustering_error if not clustering_check_pass else ""}
                    Clustering QC Job: {'PASS' if clustering_qc_check_pass else "FAIL"}
                        {clustering_qc_error if not clustering_qc_check_pass else ""}
        """

    def remapping_check_report(self, target_assembly):
        asm_res = defaultdict(dict)
        for analysis_data in self.analyses.values():
            assembly_accession = analysis_data['assembly_accession']
            vcf_extractor_result = remapping_ingestion_result = 'SKIP'
            vcf_extractor_error = remapping_ingestion_error = ""
            if assembly_accession != target_assembly:
                vcf_extractor_pass, vcf_extractor_error = self._find_log_and_check_job(
                    assembly_accession, f"{assembly_accession}_vcf_extractor.log", "vcf_extractor"
                )
                remapping_ingestion_pass, remapping_ingestion_error = self._find_log_and_check_job(
                    assembly_accession, f"{assembly_accession}*_eva_remapped.vcf_ingestion.log", "remapping_ingestion"
                )
                vcf_extractor_result = 'PASS' if vcf_extractor_pass else 'FAIL'
                remapping_ingestion_result = 'PASS' if remapping_ingestion_pass else 'FAIL'
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
            if assembly_accession != target_assembly:
                backpropagation_pass, backpropagation_error = self._find_log_and_check_job(
                    assembly_accession, f"{target_assembly}_backpropagate_to_{assembly_accession}.log", "backpropagation"
                )
                asm_res[assembly_accession]['result'] = 'PASS' if backpropagation_pass else 'FAIL'
                asm_res[assembly_accession]['error'] = backpropagation_error
            else:
                asm_res[assembly_accession]['result'] = "SKIP"
                asm_res[assembly_accession]['error'] = ""

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
            Variant load check: {self._load_vcf_job_check_result}
            Annotation check: {self._annotation_job_check_result}
            Statistics check: {self._statistics_job_check_result}
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
