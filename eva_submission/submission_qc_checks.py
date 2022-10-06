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

logger = logging_config.get_logger(__name__)


class EloadQC(Eload):
    def check_if_study_appears_in_dev(self, project_accession):
        dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/studies/{project_accession}/summary"
        response = self.get_result_from_webservice(dev_url)
        json_response = response.json()
        if self.check_if_study_present_in_response(json_response, 'id', project_accession):
            logger.info(f"{project_accession} found in DEV. Result Summary:: {json_response}")
        else:
            logger.error(f"Could not find study {project_accession} in DEV. Result Summary:: {json_response}")

    def check_if_study_appears_in_variant_browser(self, species_name, project_accession):
        dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/list?species={species_name}"
        response = self.get_result_from_webservice(dev_url)
        json_response = response.json()
        if self.check_if_study_present_in_response(json_response, 'studyId', project_accession):
            logger.info(f"{project_accession} found in DEV metadata. Result Summary:: {json_response}")
        else:
            logger.error(f"Could not find study {project_accession} in DEV metadata. Result Summary:: {json_response}")

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def get_result_from_webservice(self, url):
        return requests.get(url)

    def check_if_study_present_in_response(self, res, key, project_accession):
        if any(res) and 'response' in res and len(res['response']) > 0:
            for response in res['response']:
                if response['numTotalResults'] >= 1:
                    for result in response['result']:
                        if result[key] == project_accession:
                            return True
        return False

    def get_species_name(self, taxonomy, assembly, profile, private_config_xml_file):
        with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
            query = f"""select concat(t.taxonomy_code, '_',a.assembly_code) from evapro.taxonomy t 
                        join evapro.assembly a on a.taxonomy_id = t.taxonomy_id 
                        where t.taxonomy_id = {taxonomy} and assembly_accession='{assembly}'"""
            return get_all_results_for_query(pg_conn, query)[0][0]

    def get_browsable_files_for_study(self, profile, private_config_xml_file, project_accession):
        with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
            query = f"select filename from evapro.browsable_file where project_accession='{project_accession}'"
            return [filename for filename, in get_all_results_for_query(pg_conn, query)]

    def check_all_browsable_files_are_available_in_ftp(self, taxonomy, project_accession, vcf_files):
        logger.info(f'Browsable files in db for study {project_accession}: {vcf_files}')

        try:
            files_in_ftp = self.get_files_from_ftp(project_accession)
        except Exception as e:
            logger.error(f'Error fetching files from ftp for study {project_accession}. Error {e}')
            return

        if not files_in_ftp:
            logger.error(f'No files found in ftp for study {project_accession}')
            return

        logger.info(f'Files in ftp for study {project_accession}: {files_in_ftp}')

        for file in vcf_files:
            if file not in files_in_ftp:
                logger.error(f'{file} not found in ftp')
            if f'{file}.tbi' not in files_in_ftp:
                logger.error(f'{file}.tbi not found in ftp')
            if f'{file}.csi' not in files_in_ftp:
                logger.error(f'{file}.csi not found in ftp')

            # accessioned files will not be present for human taxonomy
            if taxonomy != 9606:
                accessioned_file = file.replace('.vcf.gz', '.accessioned.vcf.gz')
                if accessioned_file not in files_in_ftp:
                    logger.error(f'{accessioned_file} not found in ftp')
                if f'{accessioned_file}.tbi' not in files_in_ftp:
                    logger.error(f'{accessioned_file}.tbi not found in ftp')
                if f'{accessioned_file}.csi' not in files_in_ftp:
                    logger.error(f'{accessioned_file}.csi not found in ftp')

    @retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def get_files_from_ftp(self, project_accession):
        ftp = FTP('ftp.ebi.ac.uk', timeout=600)
        ftp.login()
        ftp.cwd(f'pub/databases/eva/{project_accession}')
        logger.info(f"Trying to fetch files for study {project_accession} "
                    f"from ftp location: ftp.ebi.ac.uk/pub/databases/eva/{project_accession}")
        return ftp.nlst()

    def check_if_job_completed_successfully(self, file_path):
        with open(file_path, 'r') as f:
            for line in f:
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
                if 'lines in the original VCF were skipped' in line:
                    return line

            return None

    def check_for_errors_in_case_of_job_failure(self, file_name):
        with open(file_name, 'r') as f:
            for line in f:
                if 'Encountered an error executing step' in line:
                    logger.error(f'Following error was found in log file related to execution of step: \n{line}')

    def check_if_accessioning_completed_successfully(self, project_accession, vcf_files, path_to_data_dir):
        for file in vcf_files:
            accessioning_log_files = glob.glob(f"{path_to_data_dir}/00_logs/accessioning.*{file}*.log")
            if accessioning_log_files:
                # check if accessioning job completed successfullyy
                if self.check_if_job_completed_successfully(accessioning_log_files[0]):
                    logger.info(f'Accessioning completed successfully for study {project_accession} '
                                f'and file {os.path.basename(accessioning_log_files[0])}')
                else:
                    logger.error(f'Accessioning failed for study {project_accession} '
                                 f'and file {os.path.basename(accessioning_log_files[0])}')

                    self.check_for_errors_in_case_of_job_failure(accessioning_log_files[0])

                # check if any variants were skippped while accessioning
                variants_skipped_line = self.check_if_variants_were_skipped(accessioning_log_files[0])
                if variants_skipped_line:
                    logger.error(f'Some of the variants were skipped while accessioning. '
                                 f'The following line from logs describes the total variants that were skipped.'
                                 f'\n{variants_skipped_line}')
            else:
                logger.error(f'No accessioning log file could be found for study {project_accession}')

    def check_if_variant_load_completed_successfully(self, project_accession, vcf_files, path_to_data_dir):
        for file in vcf_files:
            pipeline_log_files = glob.glob(f"{path_to_data_dir}/00_logs/pipeline.*{file}*.log")
            if pipeline_log_files:
                # check if variant load job completed successfully
                if self.check_if_job_completed_successfully(pipeline_log_files[0]):
                    logger.info(f'Variant load stage completed successfully for study {project_accession} '
                                f'and file {os.path.basename(pipeline_log_files[0])}')
                else:
                    logger.error(f'Variant load stage failed for study {project_accession} '
                                 f'and file {os.path.basename(pipeline_log_files[0])}')

                    self.check_for_errors_in_case_of_job_failure(pipeline_log_files[0])
            else:
                logger.error(f'No pipeline log file could be found for study {project_accession}')

    def check_if_browsable_files_entered_correctly_in_db(self, vcf_files, profile, private_config_xml_file,
                                                         project_accession):
        browsable_files_from_db = self.get_browsable_files_for_study(profile, private_config_xml_file,
                                                                     project_accession)
        if set(vcf_files) - set(browsable_files_from_db):
            logger.error(f"There are some VCF files missing in db. "
                         f"Missing Files : {set(vcf_files) - set(browsable_files_from_db)}")
        else:
            logger.info(f"Browsable Files entered correctly in DB. Browsable files : {vcf_files}")

    def run_qc_checks_for_submission(self):
        profile = cfg['maven']['environment']
        private_config_xml_file = cfg['maven']['settings_file']
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        path_to_data_dir = Path(cfg['projects_dir'], project_accession)
        taxonomy = self.eload_cfg.query('submission', 'taxonomy_id')
        analyses = self.eload_cfg.query('brokering', 'analyses')

        vcf_files = []
        for analysis_data in analyses.values():
            for v_files in analysis_data['vcf_files'].values():
                vcf_files.append(os.path.basename(v_files['output_vcf_file']))

        logger.info(f'----------------------------Check Browsable Files Entered Correctly-----------------------------')
        self.check_if_browsable_files_entered_correctly_in_db(vcf_files, profile, private_config_xml_file,
                                                              project_accession)

        # No accessioning check is required for human
        if taxonomy != 9606:
            logger.info(f'----------------------------Check Accessioning Job-----------------------------')
            self.check_if_accessioning_completed_successfully(project_accession, vcf_files, path_to_data_dir)

        logger.info(f'------------------------------Check Variant Load Job------------------------------')
        self.check_if_variant_load_completed_successfully(project_accession, vcf_files, path_to_data_dir)

        logger.info(f'-----------------------------Check All Files present in FTP----------------------------')
        self.check_all_browsable_files_are_available_in_ftp(taxonomy, project_accession, vcf_files)

        logger.info(f'-------------------------------Check Study appears in DEV-------------------------------')
        self.check_if_study_appears_in_dev(project_accession)

        logger.info(f'---------------------------Check Study appears in DEV Metadata---------------------------')
        for analysis_data in analyses.values():
            species_name = self.get_species_name(taxonomy, analysis_data['assembly_accession'], profile,
                                                 private_config_xml_file)
            self.check_if_study_appears_in_variant_browser(species_name, project_accession)
