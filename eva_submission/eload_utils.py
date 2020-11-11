import os
import re

import requests
from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.config import cfg

from ebi_eva_common_pyutils.logger import logging_config as log_cfg


logger = log_cfg.get_logger(__name__)


eutils_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
esearch_url = eutils_url + 'esearch.fcgi'
esummary_url = eutils_url + 'esummary.fcgi'
efetch_url = eutils_url + 'efetch.fcgi'


def retrieve_assembly_accession_from_ncbi(assembly_txt):
    """Attempt to find any assembly genebank accession base on a free text search."""
    assembly_accessions = set()
    payload = {'db': 'Assembly', 'term': '"{}"'.format(assembly_txt), 'retmode': 'JSON'}
    data = requests.get(esearch_url, params=payload).json()
    if data:
        assembly_id_list = data.get('esearchresult').get('idlist')
        payload = {'db': 'Assembly', 'id': ','.join(assembly_id_list), 'retmode': 'JSON'}
        summary_list = requests.get(esummary_url, params=payload).json()
        for assembly_id in summary_list.get('result', {}).get('uids', []):
            assembly_info = summary_list.get('result').get(assembly_id)
            if 'genbank' in assembly_info['synonym']:
                assembly_accessions.add(assembly_info['synonym']['genbank'])
    return assembly_accessions


def retrieve_species_names_from_tax_id(taxid):
    payload = {'db': 'Taxonomy', 'id': taxid}
    r = requests.get(efetch_url, params=payload)
    match = re.search('<Rank>(.+?)</Rank>', r.text, re.MULTILINE)
    rank = None
    if match:
        rank = match.group(1)
    scientific_name = None
    if rank in ['species', 'subspecies']:
        match = re.search('<ScientificName>(.+?)</ScientificName>', r.text, re.MULTILINE)
        if match:
            scientific_name = match.group(1)
        else:
            logger.error('WARNING: No species found for %s' % taxid)
    return scientific_name


def get_genome_fasta_and_report(species_name, assembly_accession, output_directory=None, overwrite=False):
    output_directory = output_directory or cfg.query('genome_downloader', 'output_directory')
    assembly = NCBIAssembly(
        assembly_accession, species_name, output_directory,
        eutils_api_key=cfg['eutils_api_key']
    )
    if not os.path.isfile(assembly.assembly_fasta_path) or not os.path.isfile(assembly.assembly_report_path) or overwrite:
        assembly.download_or_construct(overwrite=overwrite)
    return assembly.assembly_fasta_path, assembly.assembly_report_path
