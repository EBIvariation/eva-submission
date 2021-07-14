import glob
import os
import urllib
from xml.etree import ElementTree as ET

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
    except (IndexError, KeyError):
        # if there's no hold date, assume it's already been made public
        xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{project_accession}')
        attributes = xml_root.xpath('/PROJECT_SET/PROJECT/PROJECT_ATTRIBUTES/PROJECT_ATTRIBUTE')
        for attr in attributes:
            if attr.findall('TAG')[0].text == 'ENA-FIRST-PUBLIC':
                hold_date = attr.findall('VALUE')[0].text
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
