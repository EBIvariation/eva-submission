import ftplib
import os
import re
import tarfile
import tempfile
from fnmatch import fnmatch

import pymongo
import requests
from ebi_eva_common_pyutils.ncbi_utils import get_ncbi_assembly_dicts_from_term
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
]

# Name of collection in variant warehouse to check for existing VEP versions
annotation_collection_name = 'annotationMetadata_2_0'


def vep_path(version):
    return os.path.join(cfg['vep_path'], f'ensembl-vep-release-{version}/vep')


def get_vep_and_vep_cache_version(mongo_uri, db_name, assembly_accession, vep_cache_assembly_name=None):
    """
    Gets VEP and VEP cache versions for a given assembly by first checking what is already in the variant DB,
    then checking Ensembl and Ensembl Genome FTPs, otherwise returns None.
    """
    vep_version, vep_cache_version = get_vep_and_vep_cache_version_from_db(mongo_uri, db_name)
    if not vep_cache_version and not vep_version:
        vep_version, vep_cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl(
            assembly_accession, ensembl_assembly_name=vep_cache_assembly_name
        )
    else:
        vep_species, _, _ = get_species_and_assembly(assembly_accession)
    if check_vep_version_installed(vep_version):
        return vep_version, vep_cache_version, vep_species
    raise ValueError(
        f'Found VEP cache version {vep_cache_version} for assembly {assembly_accession}, '
        f'but compatible VEP version {vep_version} is not installed.'
    )


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


def get_vep_and_vep_cache_version_from_ensembl(assembly_accession, ensembl_assembly_name=None):
    vep_cache_version, ftp_source, vep_species = get_vep_cache_version_from_ftp(assembly_accession, ensembl_assembly_name)
    if not vep_cache_version:
        return None, None, None

    vep_version = get_compatible_vep_version(vep_cache_version, ftp_source)
    return vep_version, vep_cache_version, vep_species


def get_compatible_vep_version(vep_cache_version, ftp_source):
    """
    Gets VEP version compatible with given cache version, according to which FTP source the cache comes from.
    If source is Ensembl Genomes the version should be vep_cache_version + 53, otherwise the versions are equal.
    """
    return vep_cache_version + 53 if ftp_source == 'genomes' else vep_cache_version


def check_vep_version_installed(vep_version):
    return vep_version is None or os.path.exists(vep_path(vep_version))


def get_vep_cache_version_from_ftp(assembly_accession, ensembl_assembly_name=None):
    logger.info('Getting vep_cache_version from Ensembl.')
    logger.info(f'Getting species and supported assembly from Ensembl using assembly accession: {assembly_accession}')
    species_name, assembly_name, current_release_only = get_species_and_assembly(assembly_accession)
    if ensembl_assembly_name:
        assembly_name = ensembl_assembly_name
    if assembly_name is None:
        logger.info(f'No species and assembly found on Ensembl for {assembly_accession}')
        return None, None, None
    logger.info(f'Details from Ensembl for species and assembly : {species_name, assembly_name}')

    # If we're looking for an older assembly, need to search all releases to find the right one;
    # otherwise we just search the most recent release.

    # First try main Ensembl release
    ftp = get_ftp_connection(ensembl_ftp_url)
    all_releases = get_releases(ftp, '/pub', current_release_only)
    release = search_releases(ftp, all_releases, species_name, assembly_name)
    if release:
        return release, 'ensembl', species_name

    # Then try all Ensembl genomes
    genome_ftp = get_ftp_connection(ensembl_genome_ftp_url)
    for subdir in ensembl_genome_dirs:
        genome_releases = get_releases(genome_ftp, subdir, current_release_only)
        release = search_releases(genome_ftp, genome_releases, species_name, assembly_name)
        if release:
            return release, 'genomes', species_name

    logger.info(f'No VEP cache found anywhere on Ensembl FTP for {species_name} and {assembly_name}')
    return None, None


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_species_and_assembly(assembly_acc):
    """
    Returns Ensembl species name, the assembly name of the provided assembly and if the accession is the currently
     supported assembly, or None if any aren't found.
    """
    # We first need to search for the species associated with the assembly
    assembly_dicts = get_ncbi_assembly_dicts_from_term(assembly_acc)
    taxid_and_assembly_name = set([
        (assembly_dict.get('taxid'), assembly_dict.get('assemblyname'))
        for assembly_dict in assembly_dicts
        if assembly_dict.get('assemblyaccession') == assembly_acc or
           assembly_dict.get('synonym', {}).get('genbank') == assembly_acc
    ])
    # This is a search so could retrieve multiple results
    if len(taxid_and_assembly_name) != 1:
        logger.warn(f'Multiple assembly found for {assembly_acc}')
        raise ValueError(f'Cannot resolve single assembly for assembly {assembly_acc} in NCBI. ')
    taxonomy_id, assembly_name = taxid_and_assembly_name.pop()

    # Now resolve the currently supported assembly for this species in Ensembl
    url = f'https://rest.ensembl.org/info/genomes/taxonomy/{taxonomy_id}?content-type=application/json'
    response = requests.get(url)
    # This endpoint returns 500 even if taxon id is valid but just not in Ensembl, to minimise user frustration
    # we'll assume the species is just not currently supported.
    if not response.ok:
        logger.warning(f'Got {response.status_code} when trying to get species and assembly from Ensembl.')
        return None, None, None
    json_response = response.json()[0]
    return json_response['name'], assembly_name, assembly_acc == json_response['assembly_accession']


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def get_ftp_connection(url):
    ftp = ftplib.FTP(url)
    ftp.login()
    return ftp


@retry(tries=8, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def get_releases(ftp, subdir, current_only):
    """
    Get all release version numbers and paths to the releases starting from a given subdirectory.
    If current_only is True it will only return the most recent release, which can be helpful if the FTP is unreliable.
    """
    all_releases = {}
    for file in ftp.nlst(subdir):
        if "release-" in file:
            release_number = file[file.index("-") + 1:]
            all_releases.update({int(release_number): file})
    if current_only:
        current = max(all_releases)
        logger.info(f'Only getting the most recent release: {current}')
        return {current: all_releases[current]}
    return all_releases


def search_releases(ftp, all_releases, species, assembly):
    for release in sorted(all_releases, reverse=True):
        logger.info(f'Looking for vep_cache_version in release : {all_releases[release]}')
        all_species_files = get_all_species_files(ftp, all_releases.get(release))
        for f in all_species_files:
            if species in f and assembly in f:
                logger.info(f'Found vep_cache_version for {species} and {assembly}: file {f}, release {release}')
                # TODO assume if we get here we need to download the cache... is this correct?
                #  e.g. what if we've downloaded the cache for another study but VEP annotation step failed...
                download_and_extract_vep_cache(ftp, species, f)
                return release
    return None


def get_all_species_files(ftp, release):
    """
    Get all species VEP cache files for release. Note that /indexed_vep_cache is faster but not always present,
    whereas /vep is always present.
    """
    vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/indexed_vep_cache', '*.tar.gz'))
    if len(vep_cache_files) == 0:
        vep_cache_files = list(recursive_nlst(ftp, f'{release}/variation/vep', '*.tar.gz'))
    return vep_cache_files


@retry(tries=16, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def recursive_nlst(ftp, root, pattern):
    """Recursively list files starting from root and matching pattern."""
    lines = []
    ftp.dir(root, lambda content: lines.extend(content.split('\n')))
    for line in lines:
        parts = re.split(r'\s+', line)
        filename = parts[-1]
        full_path = f'{root}/{filename}'
        if parts[0][0] == 'd':  # directory
            yield from recursive_nlst(ftp, full_path, pattern)
        elif fnmatch(filename, pattern):
            yield full_path


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3), logger=logger)
def download_and_extract_vep_cache(ftp, species_name, vep_cache_file):
    tmp_dir = tempfile.TemporaryDirectory()
    destination = os.path.join(tmp_dir.name, f'{species_name}.tar.gz')
    with open(destination, 'wb+') as dest:
        ftp.retrbinary(f'RETR {vep_cache_file}', dest.write)
    with tarfile.open(destination, 'r:gz') as tar:
        tar.extractall(path=os.path.join(cfg['vep_cache_path']))
    tmp_dir.cleanup()
