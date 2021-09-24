import ftplib
import os
import re
from fnmatch import fnmatch

import pymongo
import requests
from retry import retry

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

logger = log_cfg.get_logger(__name__)

ensembl_ftp_url = 'ftp.ensembl.org'
ensembl_genome_ftp_url = 'ftp.ebi.ac.uk'
ensembl_genome_dirs = [
    'ensemblgenomes/pub/plants',
    'ensemblgenomes/pub/fungi',
    'ensemblgenomes/pub/protists',
    'ensemblgenomes/pub/metazoa',
    'ensemblgenomes/pub/bacteria',
]  # TODO should these be prioritised? note bacteria is huge and takes a while

annotation_collection_name = 'annotations_2_0'


def vep_path(version):
    return os.path.join(cfg['vep_path'], f'ensembl-vep-release-{version}/vep')


def get_vep_and_vep_cache_version(mongo_uri, db_name, assembly_accession):
    """

    :param mongo_uri:
    :param db_name:
    :param assembly_accession:
    :return:
    """
    vep_version, vep_cache_version = get_vep_and_vep_cache_version_from_db(mongo_uri, db_name)
    if not vep_cache_version and not vep_version:
        vep_version, vep_cache_version = get_vep_and_vep_cache_version_from_ensembl(assembly_accession)
    return vep_version, vep_cache_version


def get_vep_and_vep_cache_version_from_db(mongo_uri, db_name):
    logger.info(f"Getting vep_version and vep_cache_version from db: {db_name}")
    vep_version_list = []
    with pymongo.MongoClient(mongo_uri) as db:
        cursor = db[db_name][annotation_collection_name].find({})
        for document in cursor:
            vep_version_list.append((int(document['vepv']), int(document['cachev'])))
    if not vep_version_list:
        logger.info('Could not find any vep_version and vep_cache_version from db')
        return None, None
    else:
        vep_version, vep_cache_version = max(vep_version_list, key=lambda x: x[1])
        logger.info(
            f'Found following vep_version and vep_cache_versions from DB {vep_version_list}. '
            f'Latest version is {vep_version, vep_cache_version}'
        )
        return vep_version, vep_cache_version


def get_vep_and_vep_cache_version_from_ensembl(assembly_accession):
    """

    :param assembly_accession:
    :return:
    """
    vep_cache_version, ftp_source = get_vep_cache_version_from_ftp(assembly_accession)
    if not vep_cache_version:
        # TODO there's definitely some redundant logging, check this
        logger.info('Could not find VEP cache on FTP')
        return None, None

    vep_version = get_compatible_vep_version(vep_cache_version, ftp_source)

    if check_vep_version_installed(vep_version):
        # download_vep_cache(vep_cache_version)
        return vep_version, vep_cache_version

    raise ValueError(f'Found VEP cache version {vep_cache_version} for assembly {assembly_accession}, but compatible '
                     f'VEP version {vep_version} is not installed.')


def get_compatible_vep_version(vep_cache_version, ftp_source):
    # e.g. for vep version 104, the compatible cache version is 104 if it's coming from ensembl
    # but 104 - 53 = 51 if it's coming from ensembl genomes
    return vep_cache_version if ftp_source == 'ensembl' else vep_cache_version + 53


def check_vep_version_installed(vep_version):
    return os.path.exists(vep_path(vep_version))


def download_and_extract_vep_cache(vep_cache_file):
    # TODO the only real TODO here
    destination = cfg['vep_cache_path']


def get_vep_cache_version_from_ftp(assembly_accession):
    try:
        logger.info('Getting vep_cache_version from Ensembl.')
        logger.info(f'Getting species and assembly from Ensembl using assembly accession: {assembly_accession}')
        species, assembly = get_species_and_assembly(assembly_accession)
        logger.info(f'Details from Ensembl for species and assembly : {species, assembly}')

        # First try main Ensembl release
        ftp = get_ftp_connection(ensembl_ftp_url)
        all_releases = get_releases(ftp, '/pub')
        release = search_releases(ftp, all_releases, species, assembly)
        if release:
            return release, 'ensembl'

        # Then try all Ensembl genomes
        genome_ftp = get_ftp_connection(ensembl_genome_ftp_url)
        for subdir in ensembl_genome_dirs:
            genome_releases = get_releases(genome_ftp, subdir)
            release = search_releases(genome_ftp, genome_releases, species, assembly)
            if release:
                return release, 'genome'

        logger.warning(f'No VEP cache found anywhere for {species} and {assembly}!')
        return None, None
    except Exception as err:
        logger.warning(f'Encountered error while fetching vep_cache_version : {err}')
        return None, None


def search_releases(ftp, all_releases, species, assembly):
    for release in sorted(all_releases, reverse=True):
        logger.info(f'Looking for vep_cache_version in release : {all_releases.get(release)}')
        all_species_files = get_all_species_files(ftp, all_releases.get(release))
        for f in all_species_files:
            if species in f and assembly in f:
                logger.info(f'Found vep_cache_version for {species} and {assembly}: file {f}, release {release}')
                # TODO assume if we get here we need to download the cache... is this correct?
                #  e.g. if we've downloaded the cache for another study but VEP annotation step failed...
                download_and_extract_vep_cache(f)
                return release
    return None


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_species_and_assembly(assembly_accession):
    url = f'https://rest.ensembl.org/info/genomes/assembly/{assembly_accession}?content-type=application/json'
    response = requests.get(url)
    json_response = response.json()
    if "error" in json_response:
        raise Exception(json_response["error"])
    elif 'name' not in json_response or 'assembly_name' not in json_response:
        raise Exception(
            f'response from Ensembl does not contain required fields [name and assembly_name]: {json_response}')
    else:
        return json_response['name'], json_response['assembly_name']


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_ftp_connection(url):
    ftp = ftplib.FTP(url)
    ftp.login()
    return ftp


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_releases(ftp, subdir):
    all_releases = {}
    for file in ftp.nlst(subdir):
        if "release-" in file:
            release_number = file[file.index("-") + 1:]
            all_releases.update({int(release_number): file})
    return all_releases


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_all_species_files(ftp, release):
    """
    Get all species VEP cache files for release. Note that /indexed_vep_cache is faster but not always present,
    whereas /vep is always present.
    """
    vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/indexed_vep_cache', '*.tar.gz'))
    if len(vep_cache_files) == 0:
        vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/vep', '*.tar.gz'))
    return vep_cache_files


def recursive_nlst(ftp, root, pattern):
    """Recursively list files starting from root matching pattern."""
    lines = []
    ftp.dir(root, lambda l: lines.append(re.split(r'\s+', l)))
    for line in lines:
        filename = line[-1]
        full_path = f'{root}/{filename}'
        if line[0][0] == 'd':  # directory
            yield from recursive_nlst(ftp, full_path, pattern)
        elif fnmatch(filename, pattern):
            yield full_path
