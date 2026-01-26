#!/usr/bin/env python
import csv
import os
from argparse import ArgumentParser
from collections import defaultdict

import pysam
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

logger = log_cfg.get_logger(__name__)


def load_sample_to_taxonomy_mapping(tsv_file):
    """
    Load sample-to-taxonomy mapping from a TSV file.

    Args:
        tsv_file: Path to TSV file with columns: sample_name, taxonomy

    Returns:
        Tuple of (sample_to_taxonomy dict, taxonomy_to_samples dict)
    """
    sample_to_taxonomy = {}
    taxonomy_to_samples = defaultdict(list)

    with open(tsv_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) >= 2:
                sample_name = row[0].strip()
                taxonomy = row[1].strip()
                sample_to_taxonomy[sample_name] = taxonomy
                taxonomy_to_samples[taxonomy].append(sample_name)

    return sample_to_taxonomy, dict(taxonomy_to_samples)


def get_alt_allele_indices(sample_data):
    """
    Return set of ALT allele indices (1-based) that the sample carries.

    Args:
        sample_data: pysam sample record containing genotype information

    Returns:
        Set of 1-based ALT allele indices the sample has

    Examples:
        GT = (0, 1) -> {1}       (has 1st ALT)
        GT = (1, 2) -> {1, 2}    (has 1st and 2nd ALT)
        GT = (0, 0) -> {}        (homozygous reference)
        GT = (None, None) -> {}  (missing genotype)
    """
    gt = sample_data.get('GT', None)
    if gt is None:
        return set()

    alt_indices = set()
    for allele in gt:
        if allele is not None and allele > 0:
            alt_indices.add(allele)
    return alt_indices


def create_single_sample_header(original_header, sample_name):
    """
    Create a new VCF header with only a single sample.

    Args:
        original_header: The original pysam VariantHeader
        sample_name: Name of the sample to keep

    Returns:
        New pysam VariantHeader with single sample
    """
    return create_multi_sample_header(original_header, [sample_name])


def create_multi_sample_header(original_header, sample_names):
    """
    Create a new VCF header with specified samples.

    Args:
        original_header: The original pysam VariantHeader
        sample_names: List of sample names to include

    Returns:
        New pysam VariantHeader with specified samples
    """
    new_header = pysam.VariantHeader()

    # Copy all header records (contigs, INFO, FORMAT, FILTER, etc.)
    for record in original_header.records:
        new_header.add_record(record)

    # Add the specified samples
    for sample_name in sample_names:
        new_header.add_sample(sample_name)

    return new_header


def create_biallelic_record(output_file, original_record, alt_index, sample_name):
    """
    Create a biallelic variant record from a potentially multiallelic one.

    Args:
        output_file: The pysam VariantFile to create the record in
        original_record: The original pysam VariantRecord
        alt_index: 1-based index of the ALT allele to keep
        sample_name: Name of the sample

    Returns:
        New pysam VariantRecord with single ALT allele and remapped genotype
    """
    # Create new record in the output file's context
    new_record = output_file.new_record()

    # Set basic fields
    new_record.chrom = original_record.chrom
    new_record.pos = original_record.pos
    new_record.id = original_record.id
    new_record.ref = original_record.ref

    # Set single ALT allele (alt_index is 1-based, alts is 0-indexed)
    new_record.alts = (original_record.alts[alt_index - 1],)

    # Copy QUAL
    new_record.qual = original_record.qual

    # Copy FILTER
    for f in original_record.filter:
        new_record.filter.add(f)

    # Copy INFO fields, handling allele-specific fields
    for key, value in original_record.info.items():
        info_meta = original_record.header.info.get(key)
        if info_meta and info_meta.number == 'A':
            # Allele-specific field (one value per ALT allele)
            if isinstance(value, tuple) and len(value) >= alt_index:
                new_record.info[key] = (value[alt_index - 1],)
            elif not isinstance(value, tuple):
                new_record.info[key] = value
        elif info_meta and info_meta.number == 'R':
            # Reference + ALT allele specific field
            if isinstance(value, tuple) and len(value) > alt_index:
                new_record.info[key] = (value[0], value[alt_index])
            elif not isinstance(value, tuple):
                new_record.info[key] = value
        else:
            # Non-allele-specific field
            new_record.info[key] = value

    # Remap genotype: original allele indices to biallelic (0=ref, 1=alt)
    original_gt = original_record.samples[sample_name].get('GT', None)
    if original_gt is not None:
        new_gt = []
        for allele in original_gt:
            if allele is None:
                new_gt.append(None)
            elif allele == 0:
                new_gt.append(0)
            elif allele == alt_index:
                new_gt.append(1)
            else:
                # This allele is not ref and not the current alt_index,
                # so it maps to ref (0) in this biallelic representation
                new_gt.append(0)
        new_record.samples[sample_name]['GT'] = tuple(new_gt)

        # Preserve phasing if present
        if original_record.samples[sample_name].phased:
            new_record.samples[sample_name].phased = True

    # Copy other FORMAT fields for this sample
    for key in original_record.samples[sample_name].keys():
        if key == 'GT':
            continue  # Already handled above

        value = original_record.samples[sample_name].get(key)
        if value is None:
            continue

        format_meta = original_record.header.formats.get(key)
        if format_meta and format_meta.number == 'A':
            # Allele-specific format field
            if isinstance(value, tuple) and len(value) >= alt_index:
                new_record.samples[sample_name][key] = (value[alt_index - 1],)
        elif format_meta and format_meta.number == 'R':
            # Reference + ALT specific format field
            if isinstance(value, tuple) and len(value) > alt_index:
                new_record.samples[sample_name][key] = (value[0], value[alt_index])
        elif format_meta and format_meta.number == 'G':
            # Genotype-specific field (one per possible genotype)
            # For biallelic: 0/0, 0/1, 1/1 = 3 values
            # This is complex to remap correctly, skip for now
            pass
        else:
            new_record.samples[sample_name][key] = value

    return new_record


def create_biallelic_record_multi_sample(output_file, original_record, alt_index, sample_names):
    """
    Create a biallelic variant record from a potentially multiallelic one, for multiple samples.

    Args:
        output_file: The pysam VariantFile to create the record in
        original_record: The original pysam VariantRecord
        alt_index: 1-based index of the ALT allele to keep
        sample_names: List of sample names to include

    Returns:
        New pysam VariantRecord with single ALT allele and remapped genotypes for all samples
    """
    # Create new record in the output file's context
    new_record = output_file.new_record()

    # Set basic fields
    new_record.chrom = original_record.chrom
    new_record.pos = original_record.pos
    new_record.id = original_record.id
    new_record.ref = original_record.ref

    # Set single ALT allele (alt_index is 1-based, alts is 0-indexed)
    new_record.alts = (original_record.alts[alt_index - 1],)

    # Copy QUAL
    new_record.qual = original_record.qual

    # Copy FILTER
    for f in original_record.filter:
        new_record.filter.add(f)

    # Copy INFO fields, handling allele-specific fields
    for key, value in original_record.info.items():
        info_meta = original_record.header.info.get(key)
        if info_meta and info_meta.number == 'A':
            # Allele-specific field (one value per ALT allele)
            if isinstance(value, tuple) and len(value) >= alt_index:
                new_record.info[key] = (value[alt_index - 1],)
            elif not isinstance(value, tuple):
                new_record.info[key] = value
        elif info_meta and info_meta.number == 'R':
            # Reference + ALT allele specific field
            if isinstance(value, tuple) and len(value) > alt_index:
                new_record.info[key] = (value[0], value[alt_index])
            elif not isinstance(value, tuple):
                new_record.info[key] = value
        else:
            # Non-allele-specific field
            new_record.info[key] = value

    # Process each sample
    for sample_name in sample_names:
        # Remap genotype: original allele indices to biallelic (0=ref, 1=alt)
        original_gt = original_record.samples[sample_name].get('GT', None)
        if original_gt is not None:
            new_gt = []
            for allele in original_gt:
                if allele is None:
                    new_gt.append(None)
                elif allele == 0:
                    new_gt.append(0)
                elif allele == alt_index:
                    new_gt.append(1)
                else:
                    # This allele is not ref and not the current alt_index,
                    # so it maps to ref (0) in this biallelic representation
                    new_gt.append(0)
            new_record.samples[sample_name]['GT'] = tuple(new_gt)

            # Preserve phasing if present
            if original_record.samples[sample_name].phased:
                new_record.samples[sample_name].phased = True

        # Copy other FORMAT fields for this sample
        for key in original_record.samples[sample_name].keys():
            if key == 'GT':
                continue  # Already handled above

            value = original_record.samples[sample_name].get(key)
            if value is None:
                continue

            format_meta = original_record.header.formats.get(key)
            if format_meta and format_meta.number == 'A':
                # Allele-specific format field
                if isinstance(value, tuple) and len(value) >= alt_index:
                    new_record.samples[sample_name][key] = (value[alt_index - 1],)
            elif format_meta and format_meta.number == 'R':
                # Reference + ALT specific format field
                if isinstance(value, tuple) and len(value) > alt_index:
                    new_record.samples[sample_name][key] = (value[0], value[alt_index])
            elif format_meta and format_meta.number == 'G':
                # Genotype-specific field (one per possible genotype)
                # For biallelic: 0/0, 0/1, 1/1 = 3 values
                # This is complex to remap correctly, skip for now
                pass
            else:
                new_record.samples[sample_name][key] = value

    return new_record


def split_vcf_by_taxonomy(vcf_file, taxonomy_file, output_dir, prefix=None):
    """
    Split a multi-sample VCF file into per-taxonomy VCF files.

    Each output file contains all samples belonging to a taxonomy, with only variants
    where at least one sample has a non-reference genotype. Multiallelic variants
    are split into separate biallelic records.

    Args:
        vcf_file: Path to input VCF file (.vcf or .vcf.gz)
        taxonomy_file: Path to TSV file with sample-to-taxonomy mapping
        output_dir: Directory to write output files
        prefix: Optional prefix for output filenames

    Returns:
        Dict mapping taxonomy to output file paths
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load sample-to-taxonomy mapping
    sample_to_taxonomy, taxonomy_to_samples = load_sample_to_taxonomy_mapping(taxonomy_file)
    logger.info(f"Loaded taxonomy mapping: {len(sample_to_taxonomy)} samples across {len(taxonomy_to_samples)} taxonomies")

    with pysam.VariantFile(vcf_file, 'r') as vcf_in:
        vcf_samples = list(vcf_in.header.samples)

        if not vcf_samples:
            logger.warning(f"No samples found in {vcf_file}")
            return {}

        # Verify all VCF samples have taxonomy mapping
        missing_samples = [s for s in vcf_samples if s not in sample_to_taxonomy]
        if missing_samples:
            logger.warning(f"Samples without taxonomy mapping will be skipped: {', '.join(missing_samples)}")

        # Build taxonomy to VCF samples mapping (only samples present in VCF)
        taxonomy_vcf_samples = defaultdict(list)
        for sample in vcf_samples:
            if sample in sample_to_taxonomy:
                taxonomy = sample_to_taxonomy[sample]
                taxonomy_vcf_samples[taxonomy].append(sample)

        if not taxonomy_vcf_samples:
            logger.warning("No samples with taxonomy mapping found in VCF")
            return {}

        logger.info(f"Found {len(vcf_samples)} samples in VCF, mapped to {len(taxonomy_vcf_samples)} taxonomies")
        for taxonomy, samples in taxonomy_vcf_samples.items():
            logger.info(f"  Taxonomy {taxonomy}: {len(samples)} samples")

        # Create output files with multi-sample headers per taxonomy
        output_files = {}
        output_paths = {}
        for taxonomy, samples in taxonomy_vcf_samples.items():
            new_header = create_multi_sample_header(vcf_in.header, samples)
            if prefix:
                filename = f"{prefix}{taxonomy}.vcf.gz"
            else:
                filename = f"{taxonomy}.vcf.gz"
            output_path = os.path.join(output_dir, filename)
            output_paths[taxonomy] = output_path
            output_files[taxonomy] = pysam.VariantFile(output_path, 'w', header=new_header)

        # Track statistics
        total_variants = 0
        variants_per_taxonomy = {taxonomy: 0 for taxonomy in taxonomy_vcf_samples}

        # Process variants
        for record in vcf_in:
            total_variants += 1

            # For each taxonomy, find ALT alleles carried by any sample in that taxonomy
            for taxonomy, samples in taxonomy_vcf_samples.items():
                # Collect all ALT allele indices across samples in this taxonomy
                alt_indices_in_taxonomy = set()
                for sample in samples:
                    alt_indices = get_alt_allele_indices(record.samples[sample])
                    alt_indices_in_taxonomy.update(alt_indices)

                # For each ALT allele present in at least one sample, write a biallelic record
                for alt_idx in sorted(alt_indices_in_taxonomy):
                    biallelic_record = create_biallelic_record_multi_sample(
                        output_files[taxonomy],
                        record,
                        alt_index=alt_idx,
                        sample_names=samples
                    )
                    output_files[taxonomy].write(biallelic_record)
                    variants_per_taxonomy[taxonomy] += 1

        # Close all files
        for f in output_files.values():
            f.close()

        # Log statistics
        logger.info(f"Processed {total_variants} variants from {vcf_file}")
        for taxonomy in taxonomy_vcf_samples:
            logger.info(f"  Taxonomy {taxonomy}: {variants_per_taxonomy[taxonomy]} variant records written to {output_paths[taxonomy]}")

        return output_paths


def split_vcf_by_sample(vcf_file, output_dir, prefix=None):
    """
    Split a multi-sample VCF file into per-sample VCF files.

    Each output file contains only variants where the sample has a non-reference
    genotype. Multiallelic variants are split into separate biallelic records.

    Args:
        vcf_file: Path to input VCF file (.vcf or .vcf.gz)
        output_dir: Directory to write output files
        prefix: Optional prefix for output filenames

    Returns:
        Dict mapping sample names to their output file paths
    """
    os.makedirs(output_dir, exist_ok=True)

    with pysam.VariantFile(vcf_file, 'r') as vcf_in:
        samples = list(vcf_in.header.samples)

        if not samples:
            logger.warning(f"No samples found in {vcf_file}")
            return {}

        logger.info(f"Found {len(samples)} samples in {vcf_file}: {', '.join(samples)}")

        # Create output files with single-sample headers
        output_files = {}
        output_paths = {}
        for sample in samples:
            new_header = create_single_sample_header(vcf_in.header, sample)
            if prefix:
                filename = f"{prefix}{sample}.vcf.gz"
            else:
                filename = f"{sample}.vcf.gz"
            output_path = os.path.join(output_dir, filename)
            output_paths[sample] = output_path
            output_files[sample] = pysam.VariantFile(output_path, 'w', header=new_header)

        # Track statistics
        total_variants = 0
        variants_per_sample = {sample: 0 for sample in samples}

        # Process variants
        for record in vcf_in:
            total_variants += 1

            for sample in samples:
                alt_indices = get_alt_allele_indices(record.samples[sample])

                # For each ALT allele the sample carries, write a biallelic record
                for alt_idx in alt_indices:
                    biallelic_record = create_biallelic_record(
                        output_files[sample],
                        record,
                        alt_index=alt_idx,
                        sample_name=sample
                    )
                    output_files[sample].write(biallelic_record)
                    variants_per_sample[sample] += 1

        # Close all files
        for f in output_files.values():
            f.close()

        # Log statistics
        logger.info(f"Processed {total_variants} variants from {vcf_file}")
        for sample in samples:
            logger.info(f"  {sample}: {variants_per_sample[sample]} variant records written to {output_paths[sample]}")

        return output_paths


def main():
    argparse = ArgumentParser(
        description='Split a multi-sample VCF file into per-sample or per-taxonomy VCF files. '
                    'Each output file contains only variants where at least one sample has a '
                    'non-reference genotype. Multiallelic variants are split into '
                    'separate biallelic records.'
    )
    argparse.add_argument('--vcf', required=True, type=str,
                          help='Path to input VCF file (.vcf or .vcf.gz)')
    argparse.add_argument('--output-dir', required=False, type=str, default='.',
                          help='Output directory for VCF files (default: current directory)')
    argparse.add_argument('--prefix', required=False, type=str, default=None,
                          help='Optional prefix for output filenames')
    argparse.add_argument('--taxonomy-file', required=False, type=str, default=None,
                          help='Path to TSV file mapping sample names to taxonomy (columns: sample, taxonomy). '
                               'If provided, output is split by taxonomy instead of by sample.')

    args = argparse.parse_args()

    if args.taxonomy_file:
        split_vcf_by_taxonomy(
            vcf_file=args.vcf,
            taxonomy_file=args.taxonomy_file,
            output_dir=args.output_dir,
            prefix=args.prefix
        )
    else:
        split_vcf_by_sample(
            vcf_file=args.vcf,
            output_dir=args.output_dir,
            prefix=args.prefix
        )


if __name__ == "__main__":
    main()
