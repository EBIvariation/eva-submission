import gzip
import os

import pysam
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.eload_utils import cast_list
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader

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
                if len(sp_line) > 9:
                    return sp_line[9:]
                else:
                    return []
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


def get_sample_names(sample_rows):
    """
    Get sample names from either the Novel Sample section or the Pre-registered sample section
    """
    sample_names = []
    for row in sample_rows:
        if 'Sample Name' in row and row['Sample Name']:
            sample_names.append(row['Sample Name'])
        elif 'Sample ID' in row and row['Sample ID']:
            sample_names.append(row['Sample ID'])
        else:
            logger.warning('Sample Name and sample ID are both missing in row %s', row.get('row_num'))

    return sample_names


def compare_names_in_files_and_samples(files, sample_rows, analysis_alias):
    """
    Compare the sample names provided in vcf files and the one provided in a set of sample rows.
    This is meant to compare the samples and files provided for a single analysis.
    """
    has_difference = False
    sample_names_in_vcf = set()
    for file_path in files:
        # remove trailing spaces coming from the spreadsheet
        file_path = file_path.strip()
        sample_names_in_vcf.update(get_samples_from_vcf(file_path))

    sample_name_in_spreadsheet = get_sample_names(sample_rows)
    diff_submission_submitted_file = list(set(sample_name_in_spreadsheet) -
                                          set(sample_names_in_vcf))
    diff_submitted_file_submission = list(set(sample_names_in_vcf) -
                                          set(sample_name_in_spreadsheet))
    if diff_submitted_file_submission:
        has_difference = True
        logger.warning('For analysis %s Samples that appear in the VCF but not in the Metadata sheet:', analysis_alias)
        logger.warning(', '.join(sorted(cast_list(diff_submitted_file_submission))))

    if diff_submission_submitted_file:
        has_difference = True
        logger.warning('For analysis %s Samples that appear in the Metadata sheet but not in the VCF file(s):', analysis_alias)
        logger.warning(', '.join(sorted(cast_list(diff_submission_submitted_file))))

    if not has_difference:
        logger.debug('No difference found in analysis: %s\nIn spreadsheet: %s\nIn VCF files:   %s',
                     analysis_alias, sorted(sample_name_in_spreadsheet), sorted(sample_names_in_vcf))
    return has_difference, diff_submitted_file_submission, diff_submission_submitted_file


def get_vcf_file_paths(file_rows, vcf_dir):
    """Get a list of VCF file paths."""
    return [
        os.path.join(vcf_dir, file_row.get('File Name'))
        for file_row in file_rows
        if (file_row.get('File Type') and file_row.get('File Type') == 'vcf') or
           (file_row.get('File Name') and file_row.get('File Name').endswith('.vcf')) or
           (file_row.get('File Name') and file_row.get('File Name').endswith('.vcf.gz'))
    ]


def compare_spreadsheet_and_vcf(eva_files_sheet, vcf_dir):
    """
    Take a spreadsheet following EVA standard and compare the samples in it to the ones found in the VCF files
    """
    eva_xls_reader = EvaXlsxReader(eva_files_sheet)
    samples_per_analysis = eva_xls_reader.samples_per_analysis
    files_per_analysis = eva_xls_reader.files_per_analysis
    results_per_analysis_alias = {}
    if len(samples_per_analysis) == 1 and None in samples_per_analysis or \
       len(files_per_analysis) == 1 and None in files_per_analysis:
        # No analysis alias available in samples or in files. Prepare the samples and files to be used together
        samples_per_analysis[None] = eva_xls_reader.samples
        files_per_analysis[None] = eva_xls_reader.files

    for analysis_alias in samples_per_analysis:
        has_differences, diff_submitted_file_submission, diff_submission_submitted_file = compare_names_in_files_and_samples(
            get_vcf_file_paths(files_per_analysis[analysis_alias], vcf_dir),
            samples_per_analysis[analysis_alias],
            analysis_alias
        )
        results_per_analysis_alias[analysis_alias] = (
            has_differences,
            diff_submitted_file_submission,
            diff_submission_submitted_file
        )
    return results_per_analysis_alias
