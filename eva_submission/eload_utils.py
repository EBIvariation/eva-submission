import glob
import os
import urllib
from datetime import datetime
from xml.etree import ElementTree as ET

import pysam
import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.mongodb import MongoDatabase
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


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def download_file(url, dest):
    """Download a public file accessible via http or ftp."""
    urllib.request.urlretrieve(url, dest)
    urllib.request.urlcleanup()


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def check_existing_project(project_accession):
    """
    Check if a project accession exists and is public in ENA
    :param project_accession:
    :return:
    """
    try:
        download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{project_accession}')
    except requests.exceptions.HTTPError:
        return False
    return True


# Create the databases if they do not exists. Then shard them.
collections_shard_key_map = {
    "variants_2_0": (["chr", "start"], False),
    "files_2_0": (["sid", "fid", "fname"], True),
    "annotations_2_0": (["chr", "start"], False),
    "populationStatistics": (["chr", "start", "ref", "alt", "sid", "cid"], True)
}


def provision_new_database_for_variant_warehouse(db_name):
    """Create a variant warehouse database of the specified name and shared the collections"""
    # Passing the secrets_file override the password already in the uri
    db_handle = MongoDatabase(
        uri=cfg['mongodb']['mongo_admin_uri'],
        secrets_file=cfg['mongodb']['mongo_admin_secrets_file'],
        db_name=db_name
    )
    if len(db_handle.get_collection_names()) > 0:
        logger.info(f'Found existing database named {db_name}.')
    else:
        db_handle.enable_sharding()
        db_handle.shard_collections(collections_shard_key_map,
                                    collections_to_shard=collections_shard_key_map.keys())
        logger.info(f'Created new database named {db_name}.')


def detect_vcf_aggregation(vcf_file):
    """
    Detect the type of genotype aggregation done in the provided VCF file by checking the first 10 data lines
    The aggregation is determined to be "none" (meaning genotype are all present) if a GT field can be found in
    all the samples. It is determined to be "basic" if it is not "none" and an AF field or AN and AC fields are found
    in every line checked.
    Otherwise it returns None meaning that the aggregation type could not be determined.
    """
    with pysam.VariantFile(vcf_file, 'r') as vcf_in:
        samples = list(vcf_in.header.samples)
        # check that the first 10 lines have genotypes for all the samples present and if they have allele frequency
        nb_line_checked = 0
        max_line_check = 10
        gt_in_format = True
        af_in_info = True
        for vcf_rec in vcf_in:
            gt_in_format = gt_in_format and all('GT' in vcf_rec.samples.get(sample, {}) for sample in samples)
            af_in_info = af_in_info and ('AF' in vcf_rec.info or ('AC' in vcf_rec.info and 'AN' in vcf_rec.info))
            nb_line_checked += 1
            if nb_line_checked >= max_line_check:
                break
        if len(samples) > 0 and gt_in_format:
            return 'none'
        elif len(samples) == 0 and af_in_info:
            return 'basic'
        else:
            logger.error(f'Aggregation type could not be detected for {vcf_file}')
            return None
