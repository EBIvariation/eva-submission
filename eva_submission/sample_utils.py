import gzip

import pysam
from ebi_eva_common_pyutils.logger import logging_config as log_cfg


logger = log_cfg.get_logger(__name__)


def get_samples_from_vcf_manual(vcf_file):
    """
    Get the list of samples present in a single VCF file
    """
    if vcf_file.endswith('.gz'):
        open_file = gzip.open(vcf_file, 'rt')
    else:
        open_file = open(vcf_file, 'r')
    try:
        for line in open_file:
            if line.startswith('#CHROM'):
                sp_line = line.strip().split('\t')
                return sp_line[9:]
    finally:
        open_file.close()
    return []


def get_samples_from_vcf_pysam(vcf_file):
    """
    Get the list of samples present in a single VCF file
    """
    with pysam.VariantFile(vcf_file, 'r') as vcf_in:
        samples = list(vcf_in.header.samples)
    return samples


def get_samples_from_vcf(vcf_file):
    try:
        return get_samples_from_vcf_pysam(vcf_file)
    except Exception:
        return get_samples_from_vcf_manual(vcf_file)
