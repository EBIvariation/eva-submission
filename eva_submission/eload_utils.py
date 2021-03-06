import glob
import os
from urllib.parse import urlsplit

from ebi_eva_common_pyutils.reference import NCBIAssembly, NCBISequence
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_properties_from_xml_file
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
import psycopg2
from ebi_eva_common_pyutils.variation.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi
from pymongo.uri_parser import split_hosts

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


def get_metadata_creds():
    properties = get_properties_from_xml_file(cfg['maven']['environment'], cfg['maven']['settings_file'])
    pg_url = properties['eva.evapro.jdbc.url']
    pg_user = properties['eva.evapro.user']
    pg_pass = properties['eva.evapro.password']
    return pg_url, pg_user, pg_pass


def get_metadata_conn():
    pg_url, pg_user, pg_pass = get_metadata_creds()
    return psycopg2.connect(urlsplit(pg_url).path, user=pg_user, password=pg_pass)


def get_mongo_creds():
    properties = get_properties_from_xml_file(cfg['maven']['environment'], cfg['maven']['settings_file'])
    # Use the primary mongo host from configuration:
    # https://github.com/EBIvariation/configuration/blob/master/eva-maven-settings.xml#L111
    # TODO: revisit once accessioning/variant pipelines can support multiple hosts
    mongo_host = split_hosts(properties['eva.mongo.host'])[1][0]
    mongo_user = properties['eva.mongo.user']
    mongo_pass = properties['eva.mongo.passwd']
    return mongo_host, mongo_user, mongo_pass


def get_accession_pg_creds():
    properties = get_properties_from_xml_file(cfg['maven']['environment'], cfg['maven']['settings_file'])
    pg_url = properties['eva.accession.jdbc.url']
    pg_user = properties['eva.accession.user']
    pg_pass = properties['eva.accession.password']
    return pg_url, pg_user, pg_pass
