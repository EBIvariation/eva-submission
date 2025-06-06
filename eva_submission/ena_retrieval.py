import os.path

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.ena_utils import download_xml_from_ena
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query, execute_query
from retry import retry

logger = log_cfg.get_logger(__name__)


@retry(tries=3, logger=logger)
def files_from_ena(search_term):
    xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/textsearch?result=analysis&query={search_term}')
    analyses = xml_root.xpath('/ANALYSIS_SET/ANALYSIS')
    analysis_files = {}
    for analysis in analyses:
        files = analysis.xpath('FILES/FILE')
        file_dicts = []
        for file in files:
            file_dicts.append({'filename': file.attrib['filename'], 'filetype': file.attrib['filetype'],
                               'md5': file.attrib['checksum'], 'analysis_accession': analysis.attrib['accession']})
        analysis_files[analysis.attrib['accession']] = file_dicts
    return analysis_files


def get_file_id_from_md5(md5):
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        query = f"select file_id from file where file_md5='{md5}'"
        rows = get_all_results_for_query(conn, query)
        file_ids = [file_id for file_id, in rows]
        if len(file_ids) > 1:
            raise ValueError(f'Multiple file found with md5 {md5}')
        elif len(file_ids) == 0:
            return None
        return file_ids[0]


def insert_file_into_evapro(file_dict):
    filename = os.path.basename(file_dict['filename'])
    if file_dict['filename'].endswith('.vcf.gz') or file_dict['filename'].endswith('.vcf'):
        file_type = 'vcf'
    elif file_dict['filename'].endswith('.tbi'):
        file_type = 'tabix'
    else:
        raise ValueError('Unsupported file type')
    ftp_file = 'ftp.sra.ebi.ac.uk/vol1/' + file_dict['filename']

    query = ('insert into file '
             '(filename, file_md5, file_type, file_class, file_version, is_current, file_location, ftp_file) '
             f"values ('{filename}', '{file_dict['md5']}', '{file_type}', 'submitted', 1, 1, "
             f"'scratch_folder', '{ftp_file}')")
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        logger.info(f'Create file {filename} in the file table')
        execute_query(conn, query)
    file_id = get_file_id_from_md5(file_dict['md5'])
    query = (f'update file set ena_submission_file_id={file_id} where file_id={file_id}')
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        logger.info(f'Add file id in place of the ena_submission_file_id')
        execute_query(conn, query)
    return file_id


def insert_file_analysis_into_evapro(file_dict):
    query = (f"insert into analysis_file (ANALYSIS_ACCESSION,FILE_ID) "
             f"values ({file_dict['file_id']}, '{file_dict['analysis_accession']}')")
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        logger.info(f"Create file {file_dict['file_id']} in the analysis_file table for '{file_dict['analysis_accession']}'")
        execute_query(conn, query)


def remove_file_from_analysis(file_dict):
    query = (f"delete from analysis_file "
             f"where file_id={file_dict['file_id']} and analysis_accession='{file_dict['analysis_accession']}'")
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        logger.info(f"Remove file {file_dict['file_id']} from the analysis_file table for '{file_dict['analysis_accession']}'")
        execute_query(conn, query)


def difference_evapro_file_set_with_ena_for_analysis(analysis_accession, ena_list_of_file_dicts):
    query = f"select f.file_name, f.file_md5 " \
            f"from analysis_file af join file f on af.file_id=f.file_id " \
            f"where af.analysis_accession='{analysis_accession}';"
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        eva_list_of_file_dicts = [{'filename': fn, 'md5': md5} for fn, md5 in get_all_results_for_query(conn, query)]
        set_of_file_from_ena = set([d['md5'] for d in ena_list_of_file_dicts])
        set_of_file_from_eva_pro = set([d['md5'] for d in eva_list_of_file_dicts])
        if set_of_file_from_ena != set_of_file_from_eva_pro:
            logger.warning(f'File for analysis {analysis_accession} are different in ENA and EVA')
            file_specific_to_ena = set_of_file_from_ena.difference(set_of_file_from_eva_pro)
            file_specific_to_eva = set_of_file_from_eva_pro.difference(set_of_file_from_ena)
            file_dict_specific_to_ena = [file_dict for file_dict in ena_list_of_file_dicts if file_dict['md5'] in file_specific_to_ena]
            file_dict_specific_to_eva = [file_dict for file_dict in eva_list_of_file_dicts if file_dict['md5'] in file_specific_to_eva]
            for file_dict in file_dict_specific_to_ena:
                logger.warning(f"File {file_dict['filename']} exist in ENA but not in EVA.")
            for file_dict in file_dict_specific_to_eva:
                logger.warning(f"File {file_dict['filename']} exist in EVA but not in ENA.")
            return file_dict_specific_to_ena, file_dict_specific_to_eva
    return [], []


def populate_files_info_from_ena(analysis_or_project__accession):
    # Get all files from project or analysis
    analysis_files = files_from_ena(analysis_or_project__accession)
    for analysis_accession in analysis_files:
        file_specific_to_ena, file_specific_to_eva = difference_evapro_file_set_with_ena_for_analysis(analysis_accession, analysis_files[analysis_accession])
        if file_specific_to_ena:
            for file_dict in file_specific_to_ena:
                file_id = get_file_id_from_md5(file_dict['md5'])
                if not file_id:
                    file_id = insert_file_into_evapro(file_dict)
                file_dict['file_id'] = file_id
                insert_file_analysis_into_evapro(file_dict)
        if file_specific_to_eva:
            for file_dict in file_specific_to_eva:
                if file_dict['filename'].endswith('.vcf') or \
                   file_dict['filename'].endswith('.vcf.gz') or \
                   file_dict['filename'].endswith('.tbi'):
                    remove_file_from_analysis(file_dict)
