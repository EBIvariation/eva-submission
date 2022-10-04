import glob
from ftplib import FTP

import requests
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
from retry import retry

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)


def check_if_study_appears_in_dev(study_acc):
    dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/studies/{study_acc}/summary"
    response = get_result_from_webservice(dev_url)
    json_response = response.json()
    if check_if_study_present_in_response(json_response, 'id', study_acc):
        logger.info(f"Study found in DEV. Result Summary: {study_acc}: {json_response}")
    else:
        logger.error(f"Could not find study {study_acc} in DEV: \nResult Summary: {json_response}")


def check_if_study_appears_in_variant_browser(species_name):
    dev_url = f"https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/list?species={species_name}"
    response = get_result_from_webservice(dev_url)
    json_response = response.json()
    if check_if_study_present_in_response(json_response, 'studyId', study_acc):
        logger.info(f"Study found in DEV metadata. Result Summary: {study_acc}: {json_response}")
    else:
        logger.error(f"Could not find study {study_acc} in DEV metadata: \nResult Summary: {json_response}")


@retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
def get_result_from_webservice(url):
    return requests.get(url)


def check_if_study_present_in_response(res, key, study_acc):
    if any(res) and 'response' in res and len(res['response']) > 0:
        for response in res['response']:
            if response['numTotalResults'] >= 1:
                for result in response['result']:
                    if result[key] == study_acc:
                        return True
    return False


def get_species_name(taxonomy, assembly, profile, private_config_xml_file):
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        query = f"""select concat(t.taxonomy_code, '_',a.assembly_code) from evapro.taxonomy t 
                    join evapro.assembly a on a.taxonomy_id = t.taxonomy_id 
                    where t.taxonomy_id = {taxonomy} and assembly_accession='{assembly}'"""
        return get_all_results_for_query(pg_conn, query)[0][0]


def get_browsable_files_for_study(profile, private_config_xml_file, study_acc):
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        query = f"select filename from evapro.browsable_file where project_accession='{study_acc}'"
        return [filename for filename, in get_all_results_for_query(pg_conn, query)]


def check_all_browsable_files_are_available_in_ftp(taxonomy, study_acc, browsable_files):
    logger.info(f'Browsable files in db for study {study_acc}: {browsable_files}')

    try:
        files_in_ftp = get_files_from_ftp(study_acc)
    except Exception as e:
        logger.error(f'Error fetching files from ftp for study {study_acc}. Error {e}')
        return

    if not files_in_ftp:
        logger.error(f'No files found in ftp for study {study_acc}')
        return
    else:
        logger.info(f'Files in ftp for study {study_acc}: {files_in_ftp}')

    for file in browsable_files:
        if file not in files_in_ftp:
            logger.error(f'{file} not found in ftp')
        if f'{file}.tbi' not in files_in_ftp:
            logger.error(f'{file}.tbi not found in ftp')

        # accessioned files will not be present for human
        if taxonomy != 9606:
            accessioned_file = file.replace('.vcf.gz', '.accessioned.vcf.gz')
            if accessioned_file not in files_in_ftp:
                logger.error(f'{accessioned_file} not found in ftp')
            if f'{accessioned_file}.tbi' not in files_in_ftp:
                logger.error(f'{accessioned_file}.tbi not found in ftp')
            if f'{accessioned_file}.csi' not in files_in_ftp:
                logger.error(f'{accessioned_file}.csi not found in ftp')


@retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
def get_files_from_ftp(study_acc):
    ftp = FTP('ftp.ebi.ac.uk', timeout=600)
    ftp.login()
    ftp.cwd(f'pub/databases/eva/{study_acc}')
    logger.info(
        f"Trying to fetch files for study {study_acc} from ftp location: ftp.ebi.ac.uk/pub/databases/eva/{study_acc}")
    return ftp.nlst()


def check_if_job_completed_successfully(file_path):
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


def check_if_variants_were_skipped(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            if 'lines in the original VCF were skipped' in line:
                return line

        return None


def check_for_errors_in_case_of_job_failure(file_name):
    with open(file_name, 'r') as f:
        for line in f:
            if 'Encountered an error executing step' in line:
                print(f'Following error was found in log file related to execution of step: \n{line}')


def check_if_accessioning_completed_successfully(study_acc, browsable_files, path_to_data_dir):
    for file in browsable_files:
        accessioning_log_files = glob.glob(f"{path_to_data_dir}/{study_acc}/00_logs/accessioning.*{file}*.log")
        if accessioning_log_files:
            # check if accessioning job completed successfullyy
            if check_if_job_completed_successfully(accessioning_log_files[0]):
                logger.info(
                    f'Accessioning completed successfully for study {study_acc} and file {accessioning_log_files[0]}')
            else:
                logger.error(f'Accessioning failed for study {study_acc} and file {accessioning_log_files[0]}')
                check_for_errors_in_case_of_job_failure(accessioning_log_files[0])

            # check if any variants were skippped while accessioning
            variants_skipped_line = check_if_variants_were_skipped(accessioning_log_files[0])
            if variants_skipped_line:
                logger.error(f'Some of the variants were skipped while accessioning. '
                             f'The following line from logs describes the total variants that were skipped.'
                             f'\n{variants_skipped_line}')
        else:
            logger.error(f'No accessioning log file could be found for study {study_acc}')


def check_if_variant_load_completed_successfully(study_acc, browsable_files, path_to_data_dir):
    for file in browsable_files:
        pipeline_log_files = glob.glob(f"{path_to_data_dir}/{study_acc}/00_logs/pipeline.*{file}*.log")
        if pipeline_log_files:
            # check if variant load job completed successfully
            if check_if_job_completed_successfully(pipeline_log_files[0]):
                logger.info(
                    f'Variant load stage completed successfully for study {study_acc} and file {pipeline_log_files[0]}')
            else:
                logger.error(f'Variant load stage failed for study {study_acc} and file {pipeline_log_files[0]}')
                check_for_errors_in_case_of_job_failure(pipeline_log_files[0])
        else:
            logger.error(f'No pipeline log file could be found for study {study_acc}')


def run_qc_check_for_ingestion(taxonomy, assembly, study_acc, profile, private_config_xml_file, path_to_data_dir):
    check_if_study_appears_in_dev(study_acc)

    species_name = get_species_name(taxonomy, assembly, profile, private_config_xml_file)
    check_if_study_appears_in_variant_browser(species_name)

    browsable_files = get_browsable_files_for_study(profile, private_config_xml_file, study_acc)

    if browsable_files:
        check_all_browsable_files_are_available_in_ftp(taxonomy, study_acc, browsable_files)
        # No accessioning is done for human taxonomy
        if taxonomy != 9606:
            check_if_accessioning_completed_successfully(study_acc, browsable_files, path_to_data_dir)

        check_if_variant_load_completed_successfully(study_acc, browsable_files, path_to_data_dir)
    else:
        logger.error(f'No browsable files found in DB for study {study_acc}')