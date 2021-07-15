import glob
import os
import urllib
import ftplib
from datetime import datetime
from xml.etree import ElementTree as ET

import pymongo
import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
from ebi_eva_common_pyutils.reference import NCBIAssembly, NCBISequence
from ebi_eva_common_pyutils.variation.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi
from requests.auth import HTTPBasicAuth
from retry import retry

from eva_submission.assembly_taxonomy_insertion import download_xml_from_ena

logger = log_cfg.get_logger(__name__)


def get_reference_fasta_and_report(species_name, reference_accession, output_directory=None, overwrite=False):
    output_directory = output_directory or cfg.query('genome_downloader', 'output_directory')
    if NCBIAssembly.is_assembly_accession_format(reference_accession):
        assembly = NCBIAssembly(
            reference_accession, species_name, output_directory,
            eutils_api_key=cfg['eutils_api_key']
        )
        if not os.path.isfile(assembly.assembly_fasta_path) or not os.path.isfile(assembly.assembly_report_path) or overwrite:
            assembly.download_or_construct(overwrite=overwrite)
        return assembly.assembly_fasta_path, assembly.assembly_report_path
    elif NCBISequence.is_genbank_accession_format(reference_accession):
        reference = NCBISequence(reference_accession, species_name, output_directory,
                                 eutils_api_key=cfg['eutils_api_key'])
        if not os.path.isfile(reference.sequence_fasta_path) or overwrite:
            reference.download_contig_sequence_from_ncbi(genbank_only=True)
        return reference.sequence_fasta_path, None


def resolve_accession_from_text(reference_text):
    """
    :param reference_text:
    :return:
    """
    # first Check if it is an reference genome
    if NCBIAssembly.is_assembly_accession_format(reference_text):
        return [reference_text]
    # Search for a reference genome that resolve this text
    accession = retrieve_genbank_assembly_accessions_from_ncbi(reference_text)
    if accession:
        return accession

    # then check if this is a single INSDC accession
    if NCBISequence.is_genbank_accession_format(reference_text):
        return [reference_text]

    return None


def resolve_single_file_path(file_path):
    files = glob.glob(file_path)
    if len(files) == 0:
        return None
    elif len(files) > 0:
        return files[0]


def read_md5(md5_file):
    with open(md5_file) as open_file:
        md5, file_name = open_file.readline().split()
    return md5


def get_file_content(file_path):
    """
    Open a file in binary mode and close it afterwards.
    :param str file_name:
    :return: file content
    """
    with open(file_path, 'rb') as f:
        fc = f.read()
    return fc


def cast_list(l, type_to_cast=str):
    for e in l:
        yield type_to_cast(e)


def get_project_alias(project_accession):
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        query = f"select alias from evapro.project where project_accession='{project_accession}';"
        rows = get_all_results_for_query(conn, query)
    if len(rows) != 1:
        raise ValueError(f'No project alias for {project_accession} found in metadata DB.')
    return rows[0][0]


def get_hold_date_from_ena(project_accession, project_alias=None):
    """Gets hold date from ENA"""
    if not project_alias:
        project_alias = get_project_alias(project_accession)

    xml_request = f'''<SUBMISSION_SET>
           <SUBMISSION>
               <ACTIONS>
                   <ACTION>
                       <RECEIPT target="{project_alias}"/>
                  </ACTION>
              </ACTIONS>
           </SUBMISSION>
       </SUBMISSION_SET>'''
    response = requests.post(
        cfg.query('ena', 'submit_url'),
        auth=HTTPBasicAuth(cfg.query('ena', 'username'), cfg.query('ena', 'password')),
        files={'SUBMISSION': xml_request}
    )
    receipt = ET.fromstring(response.text)
    hold_date = None
    try:
        hold_date = receipt.findall('PROJECT')[0].attrib['holdUntilDate']
        hold_date = datetime.strptime(hold_date.replace(':', ''), '%Y-%m-%d%z')
    except (IndexError, KeyError):
        # if there's no hold date, assume it's already been made public
        xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{project_accession}')
        attributes = xml_root.xpath('/PROJECT_SET/PROJECT/PROJECT_ATTRIBUTES/PROJECT_ATTRIBUTE')
        for attr in attributes:
            if attr.findall('TAG')[0].text == 'ENA-FIRST-PUBLIC':
                hold_date = attr.findall('VALUE')[0].text
                hold_date = datetime.strptime(hold_date, '%Y-%m-%d')
                break
        if not hold_date:
            raise ValueError(f"Couldn't get hold date from ENA for {project_accession} ({project_alias})")
    return hold_date


def backup_file(file_name):
    """Rename the provided file by adding a '.1' at the end. If the '.1' file exists it move it to a '.2' and so on."""
    suffix = 1
    backup_name = f'{file_name}.{suffix}'
    while os.path.exists(backup_name):
        suffix += 1
        backup_name = f'{file_name}.{suffix}'

    for i in range(suffix, 1, -1):
        os.rename(f'{file_name}.{i - 1}', f'{file_name}.{i}')
    os.rename(file_name, file_name + '.1')


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def download_file(url, dest):
    """Download a public file accessible via http or ftp."""
    urllib.request.urlretrieve(url, dest)
    urllib.request.urlcleanup()



def get_vep_and_vep_cache_version(mongo_uri, db_name, coll_name, assembly_accession):
    vep_cache_version = get_vep_and_vep_cache_version_from_db(mongo_uri, db_name, coll_name)
    if not vep_cache_version:
        vep_cache_version = get_vep_and_vep_cache_version_from_ensembl(assembly_accession)
    return vep_cache_version


def get_vep_and_vep_cache_version_from_db(mongo_uri, db_name, coll_name):
    logger.info(f"Getting vep_version and vep_cache_version from db: {db_name}")
    vep_version_list = []
    with pymongo.MongoClient(mongo_uri) as db:
        cursor = db[db_name][coll_name].find({})
        for document in cursor:
            vep_version_list.append({
                "vep_version": int(document['vepv']),
                "vep_cache_version": int(document['cachev'])
            })
    if not vep_version_list:
        logger.info('Could not find any vep_version and vep_cache_version from db')
        return {
                "vep_version": None,
                "vep_cache_version": None
            }
    else:
        latest_version = max(vep_version_list, key=lambda x: x['vep_cache_version'])
        logger.info(
            f'Found following vep_version and vep_cache_versions from DB {vep_version_list}. Latest version is {latest_version}')
        return latest_version


def get_vep_and_vep_cache_version_from_ensembl(assembly_accession):
    try:
        logger.info('Getting vep and vep_cache_version from ensembl.')
        logger.info(f'Getting species and assembly from ensembl using assembly accession: {assembly_accession}')
        species_assembly = get_species_name_and_assembly(assembly_accession)
        logger.info(f'Details from Ensembl for species and assembly : {species_assembly}')

        ftp = get_ftp_connection()

        all_releases = get_releases(ftp)
        logger.info(f'fetched all releases: {all_releases}')

        for release in sorted(all_releases, reverse=True):
            logger.info(f'looking for vep and vep_cache_version in release : {all_releases.get(release)}')
            all_species_files = get_all_species_files(ftp, all_releases.get(release))
            for file in all_species_files:
                if species_assembly['species'] in file and species_assembly['assembly'] in file:
                    logger.info(
                        f'Found vep and vep_cache_version for the species and assembly : {species_assembly}, file: {file}, release: {release}')
                    return {
                        "vep_version": release,
                        "vep_cache_version": release
                    }
        logger.warning(f'could not find vep and vep_cache_version for the given species and assembly: {species_assembly}')
        return {
            "vep_version": None,
            "vep_cache_version": None
        }
    except Exception as err:
        logger.warning(f'Encountered Error while fetching vep and vep_cache_version : {err}')
        return {
            "vep_version": None,
            "vep_cache_version": None
        }


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_species_name_and_assembly(assembly_accession):
    url = f'https://rest.ensembl.org/info/genomes/assembly/{assembly_accession}?content-type=application/json'
    response = requests.get(url)
    json_response = response.json()
    if "error" in json_response:
        raise Exception(json_response["error"])
    elif 'name' not in json_response or 'assembly_name' not in json_response:
        raise Exception(
            f'response from Ensembl does not contain required fields [name and assembly_name]: {json_response}')
    else:
        return {
            "species": json_response['name'],
            "assembly": json_response['assembly_name']
        }


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_ftp_connection():
    url = "ftp.ensembl.org"
    ftp = ftplib.FTP(url)
    ftp.login()
    return ftp


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_releases(ftp):
    all_releases = {}
    for file in ftp.nlst("/pub"):
        if "release-" in file:
            release_number = file[file.index("-") + 1:]
            all_releases.update({int(release_number): file})
    return all_releases


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_all_species_files(ftp, release):
    return ftp.nlst(release + "/variation/vep")
