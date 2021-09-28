import ftplib
import os
import re
import tarfile
from fnmatch import fnmatch

import pymongo
import requests
from requests import HTTPError
from retry import retry

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg


logger = log_cfg.get_logger(__name__)

ensembl_ftp_url = 'ftp.ensembl.org'
ensembl_genome_ftp_url = 'ftp.ebi.ac.uk'
ensembl_genome_dirs = [
    'ensemblgenomes/pub/plants',
    'ensemblgenomes/pub/metazoa',
    'ensemblgenomes/pub/fungi',
    'ensemblgenomes/pub/protists',
    'ensemblgenomes/pub/bacteria',
]  # TODO should these be prioritised? note bacteria is huge and takes a while

# Name of collection in variant warehouse to check for existing VEP versions
annotation_collection_name = 'annotations_2_0'


def vep_path(version):
    return os.path.join(cfg['vep_path'], f'ensembl-vep-release-{version}/vep')


def get_vep_and_vep_cache_version(mongo_uri, db_name, assembly_accession):
    """
    Gets VEP and VEP cache versions for a given assembly by first checking what is already in the variant DB,
    then checking Ensembl and Ensembl Genome FTPs, otherwise returns None.
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
    vep_cache_version, ftp_source = get_vep_cache_version_from_ftp(assembly_accession)
    if not vep_cache_version:
        return None, None

    vep_version = get_compatible_vep_version(vep_cache_version, ftp_source)

    if check_vep_version_installed(vep_version):
        return vep_version, vep_cache_version

    raise ValueError(
        f'Found VEP cache version {vep_cache_version} for assembly {assembly_accession}, '
        f'but compatible VEP version {vep_version} is not installed.'
    )


def get_compatible_vep_version(vep_cache_version, ftp_source):
    """
    Gets VEP version compatible with given cache version, according to which FTP source the cache comes from.
    If source is Ensembl Genomes the version should be vep_cache_version + 53, otherwise the versions are equal.
    """
    return vep_cache_version + 53 if ftp_source == 'genomes' else vep_cache_version


def check_vep_version_installed(vep_version):
    return os.path.exists(vep_path(vep_version))


def get_vep_cache_version_from_ftp(assembly_accession):
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
            return release, 'genomes'

    logger.warning(f'No VEP cache found anywhere on FTP for {species} and {assembly}!')
    return None, None


def search_releases(ftp, all_releases, species, assembly):
    for release in sorted(all_releases, reverse=True):
        logger.info(f'Looking for vep_cache_version in release : {all_releases.get(release)}')
        all_species_files = get_all_species_files(ftp, all_releases.get(release))
        for f in all_species_files:
            if species in f and assembly in f:
                logger.info(f'Found vep_cache_version for {species} and {assembly}: file {f}, release {release}')
                # TODO assume if we get here we need to download the cache... is this correct?
                #  e.g. what if we've downloaded the cache for another study but VEP annotation step failed...
                download_and_extract_vep_cache(ftp, species, f)
                return release
    return None


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_species_and_assembly(assembly_accession):
    url = f'https://rest.ensembl.org/info/genomes/assembly/{assembly_accession}?content-type=application/json'
    response = requests.get(url)
    json_response = response.json()
    if 'error' in json_response:
        raise HTTPError(response.status_code, json_response['error'])
    elif 'name' not in json_response or 'assembly_name' not in json_response:
        raise ValueError(f'Response from Ensembl does not contain required fields [name and assembly_name]: '
                         f'{json_response}')
    else:
        return json_response['name'], json_response['assembly_name']


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def get_ftp_connection(url):
    ftp = ftplib.FTP(url)
    ftp.login()
    return ftp


@retry(tries=8, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def get_releases(ftp, subdir):
    all_releases = {}
    for file in ftp.nlst(subdir):
        if "release-" in file:
            release_number = file[file.index("-") + 1:]
            all_releases.update({int(release_number): file})
    return all_releases


def get_all_species_files(ftp, release):
    """
    Get all species VEP cache files for release. Note that /indexed_vep_cache is faster but not always present,
    whereas /vep is always present.
    """
    vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/indexed_vep_cache', '*.tar.gz'))
    if len(vep_cache_files) == 0:
        vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/vep', '*.tar.gz'))
    return vep_cache_files


@retry(tries=8, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def recursive_nlst(ftp, root, pattern):
    """Recursively list files starting from root and matching pattern."""
    lines = []
    ftp.dir(root, lambda l: lines.append(re.split(r'\s+', l)))
    for line in lines:
        filename = line[-1]
        full_path = f'{root}/{filename}'
        if line[0][0] == 'd':  # directory
            yield from recursive_nlst(ftp, full_path, pattern)
        elif fnmatch(filename, pattern):
            yield full_path


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def download_and_extract_vep_cache(ftp, species_name, vep_cache_file):
    destination = os.path.join(cfg['vep_cache_path'], f'{species_name}.tar.gz')
    with open(destination, 'wb+') as dest:
        ftp.retrbinary(f'RETR {vep_cache_file}', dest.write)
    with tarfile.open(destination, 'r:gz') as tar:
        tar.extractall(path=os.path.join(cfg['vep_cache_path'], species_name))
    # TODO need to remove one level of nesting...
    os.remove(destination)
