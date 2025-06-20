import glob
import os
from collections import defaultdict
from ftplib import FTP
from functools import cached_property
from pathlib import Path

import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_submission.qc_utils import did_job_complete_successfully_from_log, get_failed_job_or_step_name
from requests import HTTPError
from retry import retry

from eva_submission.eload_submission import Eload
from eva_submission.submission_config import EloadConfig


def rreplace(s, old, new, occurrence=1):
    li = s.rsplit(old, occurrence)
    return new.join(li)

class EloadQC(Eload):
    config_section = 'qc_checks'  # top-level config key

    # Possible QC results
    PASS = 'PASS'
    FAIL = 'FAIL'
    SKIP = 'SKIP'
    PASS_WITH_WARNING = 'PASS with Warning (Manual Check Required)'
    # QC statuses that are considered successful
    SUCCESSFUL_RESULTS = {PASS, SKIP, PASS_WITH_WARNING}

    def __init__(self, eload_number, config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        self.profile = cfg['maven']['environment']
        self.private_config_xml_file = cfg['maven']['settings_file']
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        self.path_to_data_dir = Path(cfg['projects_dir'], self.project_accession)
        self.path_to_logs_dir = os.path.join(self.path_to_data_dir, '00_logs')
        # If the project folder does not exist then it is located in the eload folder
        if not os.path.exists(self.path_to_logs_dir):
            self.path_to_logs_dir = os.path.join(self.eload_dir, '00_logs')
        self.taxonomy = self.eload_cfg.query('submission', 'taxonomy_id')
        self.analyses = self.eload_cfg.query('brokering', 'analyses')

    @cached_property
    def vcf_files(self):
        vcf_files = []
        for analysis_data in self.analyses.values():
            for v_files in analysis_data['vcf_files'].values():
                vcf_files.append(os.path.basename(v_files['output_vcf_file']))
        return vcf_files

    @cached_property
    def analysis_to_file_names(self):
        analysis_to_file_names = {}
        for analysis_alias, analysis_accession in self.eload_cfg.query('brokering', 'ena', 'ANALYSIS').items():
            # Find the files associated with this analysis
            analysis_to_file_names[analysis_accession] = [
                os.path.basename(f) for f in self.analyses.get(analysis_alias).get('vcf_files')
            ]
        return analysis_to_file_names

    ###
    # Helper methods
    ###

    def _check_if_study_appears_in_variant_browser(self, species_name):
        url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/list?species={species_name}"
        try:
            json_response = self._get_result_from_webservice(url)
        except HTTPError as e:
            self.error(str(e))
            json_response = {}
        if self._check_if_study_present_in_response(json_response, 'studyId'):
            return True
        else:
            return False

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def _get_result_from_webservice(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def _check_if_study_present_in_response(self, res, key):
        if any(res) and 'response' in res and len(res['response']) > 0:
            for response in res['response']:
                if response['numTotalResults'] >= 1:
                    for result in response['result']:
                        if result[key] == self.project_accession:
                            return True
        return False

    def _get_species_name(self, assembly):
        with get_metadata_connection_handle(self.profile, self.private_config_xml_file) as pg_conn:
            query = f"""select concat(t.taxonomy_code, '_',a.assembly_code) from evapro.taxonomy t 
                        join evapro.assembly a on a.taxonomy_id = t.taxonomy_id 
                        where t.taxonomy_id = {self.taxonomy} and assembly_accession='{assembly}'"""
            return get_all_results_for_query(pg_conn, query)[0][0]

    def _get_browsable_files_for_study(self):
        with get_metadata_connection_handle(self.profile, self.private_config_xml_file) as pg_conn:
            query = f"select filename from evapro.browsable_file where project_accession='{self.project_accession}'"
            return [filename for filename, in get_all_results_for_query(pg_conn, query)]

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def _get_files_from_ftp(self, project_accession):
        ftp = FTP('ftp.ebi.ac.uk', timeout=600)
        ftp.login()
        ftp.cwd(f'pub/databases/eva/{project_accession}')
        return ftp.nlst()

    def _check_if_variants_were_skipped_in_log(self, file_path):
        with open(file_path, 'r') as f:
            variants_skipped = -1
            for line in f:
                if "Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched" in line:
                    variants_skipped = None
                if 'lines in the original VCF were skipped' in line:
                    variants_skipped = line.strip().split(":")[-1].strip().split(" ")[0].strip()

            return variants_skipped

    def _check_multiple_logs(self, search_unit, log_patterns, job_types):
        """
        Go through the list of provided logs and search for the given job types.
        It returns a positive result if at least one if these jobs is found to pass, similar to the any() function.
        The search_unit is group for which this search is perform, typically a file name or analysis accession
        Returns a tuple with the test result as boolean and the last error message if none of the jobs are found.
        """
        assert len(log_patterns) == len(job_types)
        any_pass = False
        last_error = f'No log checked for {search_unit}'
        for log_pattern, job_type in zip(log_patterns, job_types):
            check_pass, last_error = self._find_log_and_check_job(search_unit, log_pattern, job_type)
            any_pass = any_pass or check_pass
            if any_pass:
                break
        return any_pass, last_error

    def _find_log_and_check_job(self, search_unit, log_file_pattern, job_type):
        """
        Find a log file using the provided log_file_pattern and check if the specified job_type was run successfully.
        The search_unit is group for which this search is perform, typically a file name or analysis accession
        Returns a tuple with the test result as boolean and optional error message
        """
        log_files = glob.glob(os.path.join(self.path_to_logs_dir, log_file_pattern))
        report_text = ""
        if log_files:
            # check if job completed successfully
            if not did_job_complete_successfully_from_log(log_files[0], job_type):
                report_text += f"{job_type} failed job/step : {get_failed_job_or_step_name(log_files[0])}"
                job_passed = False
            else:
                job_passed = True
        else:
            report_text += f"{job_type} error : No {job_type} log file found for {search_unit}"
            job_passed = False
        return job_passed, report_text

    ###
    # Reporting methods
    ###

    @staticmethod
    def _report_for_human():
        result = EloadQC.SKIP
        report = 'Success: SKIPPED (human taxonomy)'
        return result, report

    @staticmethod
    def _report_did_not_run():
        result = EloadQC.FAIL
        report = 'Success: DID NOT RUN'
        return result, report

    @staticmethod
    def _report_for_log(failed_unit):
        """Create a result string and a detailed report based on the error reported in failed unit"""
        result = EloadQC.PASS if not failed_unit else EloadQC.FAIL
        report = f"""Success: {result}"""
        if failed_unit:
            report += f"""
            Errors:"""
            for unit, value in failed_unit.items():
                report += f"""
                {unit} - {value}"""
        return result, report

    ###
    # Check methods
    ###

    def check_if_study_appears(self):
        url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/studies/{self.project_accession}/summary"
        try:
            json_response = self._get_result_from_webservice(url)
        except HTTPError as e:
            self.error(str(e))
            json_response = {}
        if self._check_if_study_present_in_response(json_response, 'id'):
            result = EloadQC.PASS
        else:
            result = EloadQC.FAIL

        report = f"""Success: {result}"""
        return result, report

    def check_if_study_appears_in_metadata(self):
        missing_assemblies = []
        for analysis_data in self.analyses.values():
            species_name = self._get_species_name(analysis_data['assembly_accession'])
            if not self._check_if_study_appears_in_variant_browser(species_name):
                missing_assemblies.append(f"{species_name}({analysis_data['assembly_accession']})")

        result = EloadQC.PASS if not missing_assemblies else EloadQC.FAIL
        report = f"""Success: {result}
                missing assemblies: {missing_assemblies if missing_assemblies else None}"""
        return result, report

    def check_all_browsable_files_are_available_in_ftp(self):
        try:
            files_in_ftp = self._get_files_from_ftp(self.project_accession)
        except Exception as e:
            self.error(f"Error fetching files from ftp for study {self.project_accession}. Exception  {e}")
            result = EloadQC.FAIL
            report = f"""Error: Error fetching files from ftp for study {self.project_accession}"""
            return result, report

        if not files_in_ftp:
            self.error(f"No file found in ftp for study {self.project_accession}")
            result = EloadQC.FAIL
            report = f"""Error: No files found in FTP for study {self.project_accession}"""
            return result, report

        missing_files = []

        for file in self.vcf_files:
            no_ext_file, _ = os.path.splitext(file)
            if file not in files_in_ftp:
                missing_files.append(file)
            if f'{file}.csi' not in files_in_ftp and f'{no_ext_file}.csi' not in files_in_ftp:
                missing_files.append(f'{file}.csi or {no_ext_file}.csi')

            # accessioned files will not be present for human taxonomy
            if self.taxonomy != 9606:
                accessioned_file = rreplace(file, '.vcf.gz', '.accessioned.vcf.gz')
                no_ext_accessioned_file, _ = os.path.splitext(accessioned_file)
                if accessioned_file not in files_in_ftp:
                    missing_files.append(accessioned_file)
                if f'{accessioned_file}.csi' not in files_in_ftp and \
                        f'{no_ext_accessioned_file}.csi' not in files_in_ftp:
                    missing_files.append(f'{accessioned_file}.csi or {no_ext_accessioned_file}.csi')

        result = EloadQC.PASS if not missing_files else EloadQC.FAIL
        report = f"""Success: {result} 
                Missing files: {missing_files if missing_files else None}"""
        return result, report

    def check_if_accessioning_completed_successfully(self):
        # No accessioning check is required for human
        if self.taxonomy == 9606:
            return self._report_for_human()

        failed_files = {}
        for file in self.vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_logs_dir}/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if accessioning job completed successfully
                if not did_job_complete_successfully_from_log(accessioning_log_files[0], 'accession'):
                    failed_files[
                        file] = f"failed job/step : {get_failed_job_or_step_name(accessioning_log_files[0])}"
            else:
                failed_files[file] = f"Accessioning Error : No accessioning file found for {file}"

        result = EloadQC.PASS if not failed_files else EloadQC.FAIL
        report = f"""Success: {result}"""
        if failed_files:
            report += f"""
            failed_files:"""
            for file, value in failed_files.items():
                report += f"""
                {file} - {value}"""

        return result, report

    def check_if_variant_load_completed_successfully(self):
        failed_files = {}
        for file_name in self.vcf_files:
            file_pass, last_error = self._check_multiple_logs(
                file_name,
                [f"pipeline.*{file_name}*.log", f"load_variants.*{file_name}*.log"],
                ["variant_load", "load_vcf"])
            if not file_pass:
                failed_files[file_name] = last_error
        return self._report_for_log(failed_files)

    def check_if_acc_load_completed_successfully(self):
        # No accessioning load check is required for human
        if self.taxonomy == 9606:
            return self._report_for_human()
        failed_files = {}
        for file_name in self.vcf_files:
            file_pass, last_error = self._check_multiple_logs(
                file_name,
                [f"pipeline.*{file_name}*.log", f"acc_import.*{file_name}*.log"],
                ["variant_load", "acc_import"])
            if not file_pass:
                failed_files[file_name] = last_error
        return self._report_for_log(failed_files)

    def check_if_vep_completed_successfully(self):
        failed_analysis = {}
        any_vep_run = False
        for analysis_alias, analysis_accession in self.eload_cfg.query('brokering', 'ena', 'ANALYSIS').items():
            # annotation only happens if a VEP cache can be found
            assembly_accession = self.eload_cfg.query('brokering', 'analyses', analysis_alias, 'assembly_accession')
            if self.eload_cfg.query('ingestion', 'vep', assembly_accession, 'cache_version') is not None:
                any_vep_run = True
                logs_to_check = []
                jobs_to_check = []
                for file_name in self.analysis_to_file_names[analysis_accession]:
                    logs_to_check.append(f"pipeline.*{file_name}*.log")
                    jobs_to_check.append("variant_load")
                    logs_to_check.append(f"annotation.*{analysis_accession}*.log")
                    jobs_to_check.append("annotate_variants")
                analysis_pass, last_error = self._check_multiple_logs(analysis_accession, logs_to_check, jobs_to_check)
                if not analysis_pass:
                    failed_analysis[analysis_accession] = last_error
        if any_vep_run:
            return self._report_for_log(failed_analysis)
        else:
            return EloadQC.SKIP, f"""Annotation result - SKIPPED (no VEP cache)"""

    def check_if_variant_statistic_completed_successfully(self):
        failed_analysis = {}
        any_stats_run = False
        for analysis_alias, analysis_accession in self.eload_cfg.query('brokering', 'ena', 'ANALYSIS').items():
            if self.eload_cfg.query('ingestion', 'aggregation', analysis_accession) == 'none':
                any_stats_run = True
                logs_to_check = []
                jobs_to_check = []
                for file_name in self.analysis_to_file_names[analysis_accession]:
                    logs_to_check.append(f"pipeline.*{file_name}*.log")
                    jobs_to_check.append("variant_load")
                logs_to_check.extend([
                    f"statistics.*{analysis_accession}*.log",
                    f"variant.statistics.{analysis_accession}.log"
                ])
                jobs_to_check.extend(["calculate_statistics", "variant-stats"])
                analysis_pass, last_error = self._check_multiple_logs(analysis_accession, logs_to_check, jobs_to_check)
                if not analysis_pass:
                    failed_analysis[analysis_accession] = last_error
        if any_stats_run:
            return self._report_for_log(failed_analysis)
        else:
            return EloadQC.SKIP, f"""Variant statistics result - SKIPPED (aggregated VCF)"""

    def check_if_study_statistic_completed_successfully(self):
        failed_analysis = {}
        any_stats_run = False
        for analysis_alias, analysis_accession in self.eload_cfg.query('brokering', 'ena', 'ANALYSIS').items():
            if self.eload_cfg.query('ingestion', 'aggregation', analysis_accession) == 'none':
                any_stats_run = True
                logs_to_check = []
                jobs_to_check = []
                for file_name in self.analysis_to_file_names[analysis_accession]:
                    logs_to_check.append(f"pipeline.*{file_name}*.log")
                    jobs_to_check.append("variant_load")
                logs_to_check.extend([
                    f"statistics.*{analysis_accession}*.log",
                    f"study.statistics.{analysis_accession}.log"
                ])
                jobs_to_check.extend(["calculate_statistics", "file-stats"])
                analysis_pass, last_error = self._check_multiple_logs(analysis_accession, logs_to_check, jobs_to_check)
                if not analysis_pass:
                    failed_analysis[analysis_accession] = last_error
        if any_stats_run:
            return self._report_for_log(failed_analysis)
        else:
            return EloadQC.SKIP, f"""Study statistics result - SKIPPED (aggregated VCF)"""

    def check_if_variants_were_skipped_while_accessioning(self):
        # No accessioning check is required for human
        if self.taxonomy == 9606:
            return self._report_for_human()
        failed_files = {}
        for file in self.vcf_files:
            accessioning_log_files = glob.glob(f"{self.path_to_logs_dir}/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if any variants were skipped while accessioning
                variants_skipped = self._check_if_variants_were_skipped_in_log(accessioning_log_files[0])
                if variants_skipped:
                    if variants_skipped == -1:
                        failed_files[file] = f"could not retrieve skipped variants count"
                    else:
                        failed_files[file] = f"{variants_skipped} variants skipped"
            else:
                failed_files[file] = f"Accessioning Error : No accessioning file found for {file}"

        result = EloadQC.PASS if not failed_files else EloadQC.PASS_WITH_WARNING
        report = f"""Success: {result}"""
        if failed_files:
            report += f"""
            Failures:"""
            for file, value in failed_files.items():
                report += f"""
                {file} - {value}"""

        return result, report

    def check_if_browsable_files_entered_correctly_in_db(self):
        browsable_files_from_db = self._get_browsable_files_for_study()
        missing_files = set(self.vcf_files) - set(browsable_files_from_db)
        result = EloadQC.PASS if len(missing_files) == 0 else EloadQC.FAIL
        report = f"""Success : {result}
            Expected files: {self.vcf_files}
            Missing files: {missing_files if missing_files else 'None'}"""
        return result, report

    def clustering_check_report(self):
        if self.taxonomy == 9606:
            return self._report_for_human()
        target_assembly = self.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly')
        if not target_assembly:
            return self._report_did_not_run()
        clustering_check_pass, clustering_error = self._find_log_and_check_job(
            target_assembly, f'{target_assembly}_clustering.log', 'clustering'
        )
        clustering_qc_check_pass, clustering_qc_error = self._find_log_and_check_job(
            target_assembly, f'{target_assembly}_clustering_qc.log', 'clustering_qc'
        )

        if clustering_check_pass and clustering_qc_check_pass:
            result = EloadQC.PASS
        else:
            result = EloadQC.FAIL

        report = f"""Clustering Job: {EloadQC.PASS if clustering_check_pass else EloadQC.FAIL} - {clustering_error if not clustering_check_pass else "No error"}
            Clustering QC Job: {EloadQC.PASS if clustering_qc_check_pass else EloadQC.FAIL} - {clustering_qc_error if not clustering_qc_check_pass else "No error"}"""
        return result, report

    def remapping_check_report(self):
        if self.taxonomy == 9606:
            return self._report_for_human()
        target_assembly = self.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly')
        if not target_assembly:
            return self._report_did_not_run()
        asm_res = defaultdict(dict)
        for analysis_data in self.analyses.values():
            assembly_accession = analysis_data['assembly_accession']
            vcf_extractor_result = remapping_ingestion_result = EloadQC.SKIP
            vcf_extractor_error = remapping_ingestion_error = ""
            if assembly_accession != target_assembly:
                vcf_extractor_pass, vcf_extractor_error = self._find_log_and_check_job(
                    assembly_accession, f"{assembly_accession}_vcf_extractor.log", "vcf_extractor"
                )
                remapping_ingestion_pass, remapping_ingestion_error = self._find_log_and_check_job(
                    assembly_accession, f"{assembly_accession}*_eva_remapped.vcf_ingestion.log", "remapping_ingestion"
                )
                vcf_extractor_result = EloadQC.PASS if vcf_extractor_pass else EloadQC.FAIL
                remapping_ingestion_result = EloadQC.PASS if remapping_ingestion_pass else EloadQC.FAIL
            asm_res[assembly_accession]['vcf_extractor_result'] = vcf_extractor_result
            asm_res[assembly_accession]['vcf_extractor_error'] = vcf_extractor_error
            asm_res[assembly_accession]['remapping_ingestion_result'] = remapping_ingestion_result
            asm_res[assembly_accession]['remapping_ingestion_error'] = remapping_ingestion_error

        result = EloadQC.PASS

        report_lines = []
        for asm, res in asm_res.items():
            vcf_ext_res = res['vcf_extractor_result']
            vcf_ext_err = 'No Error' if res['vcf_extractor_error'] == "" else res['vcf_extractor_error']
            remap_ingest_res = res['remapping_ingestion_result']
            remap_ingest_err = 'No Error' if res['remapping_ingestion_error'] == "" \
                else res['remapping_ingestion_error']
            if vcf_ext_res == EloadQC.FAIL or remap_ingest_res == EloadQC.FAIL:
                result = EloadQC.FAIL

            report_lines.append(f"""Source assembly {asm}:
                - vcf_extractor_result : {vcf_ext_res} - {vcf_ext_err}
                - remapping_ingestion_result: {remap_ingest_res} - {remap_ingest_err}""")
        return result, '\n            '.join(report_lines)

    def backpropagation_check_report(self):
        if self.taxonomy == 9606:
            return self._report_for_human()
        target_assembly = self.eload_cfg.query('ingestion', 'remap_and_cluster', 'target_assembly')
        if not target_assembly:
            return self._report_did_not_run()
        asm_res = defaultdict(dict)
        for analysis_data in self.analyses.values():
            assembly_accession = analysis_data['assembly_accession']
            if assembly_accession != target_assembly:
                backpropagation_pass, backpropagation_error = self._find_log_and_check_job(
                    assembly_accession, f"{target_assembly}_backpropagate_to_{assembly_accession}.log",
                    "backpropagation"
                )
                asm_res[assembly_accession]['result'] = EloadQC.PASS if backpropagation_pass else EloadQC.FAIL
                asm_res[assembly_accession]['error'] = backpropagation_error
            else:
                asm_res[assembly_accession]['result'] = EloadQC.SKIP
                asm_res[assembly_accession]['error'] = ""

        result = EloadQC.PASS

        report_lines = []
        for asm, bckp_result in asm_res.items():
            res = bckp_result['result']
            err = 'No Error' if bckp_result['error'] == '' else bckp_result['error']
            if res == EloadQC.FAIL:
                result = EloadQC.FAIL
            report_lines.append(f"""Backpropagation result to {asm}: {res} - {err}""")

        return result, '\n            '.join(report_lines)

    def run_qc_checks_for_submission(self):
        """Collect information from different qc methods format and write the report."""
        browsable_files_result, browsable_files_report = self.check_if_browsable_files_entered_correctly_in_db()

        accessioning_job_result, accessioning_job_report = self.check_if_accessioning_completed_successfully()
        variants_skipped_result, variants_skipped_report = self.check_if_variants_were_skipped_while_accessioning()

        variant_load_result, variant_load_report = self.check_if_variant_load_completed_successfully()
        annotation_result, annotation_report = self.check_if_vep_completed_successfully()
        variant_statistic_result, variant_statistic_report = self.check_if_variant_statistic_completed_successfully()
        study_statistic_result, study_statistic_report = self.check_if_study_statistic_completed_successfully()
        acc_import_result, acc_import_report = self.check_if_acc_load_completed_successfully()

        clustering_check_result, clustering_check_report = self.clustering_check_report()
        remapping_check_result, remapping_check_report = self.remapping_check_report()
        backpropagation_check_result, backpropagation_check_report = self.backpropagation_check_report()
        ftp_check_result, ftp_check_report = self.check_all_browsable_files_are_available_in_ftp()

        study_check_result, study_check_report = self.check_if_study_appears()

        study_metadata_check_result, study_metadata_check_report = self.check_if_study_appears_in_metadata()

        result_summary = {
            'browsable_files': browsable_files_result,
            'accessioning': accessioning_job_result,
            'variants_skipped_accessioning': variants_skipped_result,
            'variant_load': variant_load_result,
            'annotation': annotation_result,
            'variant_stats': variant_statistic_result,
            'study_stats': study_statistic_result,
            'accession_import': acc_import_result,
            'remapping': remapping_check_result,
            'clustering': clustering_check_result,
            'back-propogation': backpropagation_check_result,
            'ftp': ftp_check_result,
            'study_webservice': study_check_result,
            'study_metadata': study_metadata_check_result,
        }
        self.eload_cfg.set(EloadQC.config_section, value=result_summary)

        report = f"""
        QC Result Summary:
        ------------------
        Browsable files check: {browsable_files_result}
        Accessioning job check: {accessioning_job_result}
        Variants Skipped accessioning check: {variants_skipped_result}
        Variant load and Accession Import check:
            Variant load check: {variant_load_result}
            Annotation check: {annotation_result}
            Variant Statistics check: {variant_statistic_result}
            Study Statistics check: {study_statistic_result}
            Accession Import check: {acc_import_result}
        Remapping and Clustering Check:
            Remapping check: {remapping_check_result}
            Clustering check: {clustering_check_result}
            Back-propogation check: {backpropagation_check_result}
        FTP check: {ftp_check_result}
        Study check: {study_check_result}
        Study metadata check: {study_metadata_check_result}
        
        QC Details:
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
        Annotation check: 
            {annotation_report}
        ----------------------------------
        Variant Statistics check: 
            {variant_statistic_report}
        ----------------------------------
        Study Statistics check: 
            {study_statistic_report}
        ----------------------------------
        Accession Import check: 
            {acc_import_report}
        ----------------------------------
        Remapping Check:
            {remapping_check_report}
        ----------------------------------
        Clustering check:
            {clustering_check_report}
        ----------------------------------
        Backpropagation check:
            {backpropagation_check_report}
        ----------------------------------
        FTP check:
            {ftp_check_report}
        ----------------------------------
        Study check:
            {study_check_report}
        ----------------------------------
        Study metadata check:
            {study_metadata_check_report}
        ----------------------------------
        """

        print(report)

        return report
