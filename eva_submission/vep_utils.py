import ftplib

import pymongo
import requests
from retry import retry

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

logger = log_cfg.get_logger(__name__)


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
