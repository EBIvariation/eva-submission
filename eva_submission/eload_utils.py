import glob
import os

from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

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


