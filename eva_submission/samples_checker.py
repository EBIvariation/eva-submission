import logging

import pysam
import os
import sys

from ebi_eva_common_pyutils.logger import logging_config as log_cfg


sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from eva_submission.xlsreader import EVAXLSReader


logger = log_cfg.get_logger(__name__)


def get_samples_from_vcf(vcf_file):
    """
    Get the list of samples present in a single VCF file
    """
    with pysam.VariantFile(vcf_file, 'r') as vcf_in:
        samples = list(vcf_in.header.samples)
    return samples


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
            logging.warning('Sample Name and sample ID are both missing in row %s', row.get('row_num'))

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
        logger.warning(', '.join(sorted(diff_submitted_file_submission)))

    if diff_submission_submitted_file:
        has_difference = True
        logger.warning('For analysis %s Samples that appear in the Metadata sheet but not in the VCF file(s):', analysis_alias)
        logger.warning(', '.join(sorted(diff_submission_submitted_file)))

    if not has_difference:
        logger.debug('No difference found in analysis: %s\nIn spreadsheet: %s\nIn VCF files:   %s',
                     analysis_alias, sorted(sample_name_in_spreadsheet), sorted(sample_names_in_vcf))
    return has_difference


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
    eva_xls_reader = EVAXLSReader(eva_files_sheet)
    samples_per_analysis = eva_xls_reader.samples_per_analysis
    files_per_analysis = eva_xls_reader.files_per_analysis
    has_differences = False
    if len(samples_per_analysis) == 1 and None in samples_per_analysis or \
       len(files_per_analysis) == 1 and None in files_per_analysis:
        # No analysis alias available in samples or in files. Process all samples/files together
        has_differences = compare_names_in_files_and_samples(
            get_vcf_file_paths(eva_xls_reader.files, vcf_dir),
            eva_xls_reader.files,
            None
        )

    else:
        for analysis_alias in samples_per_analysis:
            has_differences = has_differences or compare_names_in_files_and_samples(
                get_vcf_file_paths(files_per_analysis[analysis_alias], vcf_dir),
                samples_per_analysis[analysis_alias],
                analysis_alias
            )
    if not has_differences:
        logger.info('No differences found between the samples in the Metadata sheet and the submitted VCF file(s)!')
    logger.info('Samples checking completed!')
