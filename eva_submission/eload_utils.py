import glob
import os
from urllib.parse import urlsplit

from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_properties_from_xml_file
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
import psycopg2
from pymongo.uri_parser import split_hosts

logger = log_cfg.get_logger(__name__)


def get_genome_fasta_and_report(species_name, assembly_accession, output_directory=None, overwrite=False):
    output_directory = output_directory or cfg.query('genome_downloader', 'output_directory')
    assembly = NCBIAssembly(
        assembly_accession, species_name, output_directory,
        eutils_api_key=cfg['eutils_api_key']
    )
    if not os.path.isfile(assembly.assembly_fasta_path) or not os.path.isfile(assembly.assembly_report_path) or overwrite:
        assembly.download_or_construct(overwrite=overwrite)
    return assembly.assembly_fasta_path, assembly.assembly_report_path


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
