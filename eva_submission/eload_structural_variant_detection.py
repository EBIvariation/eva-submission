#!/usr/bin/env python
import re
import gzip
from argparse import ArgumentParser


def detect_structural_variant(vcf_file, output_vcf):
    has_sv_per_vcf = False
    # Ref: https://samtools.github.io/hts-specs/VCFv4.3.pdf (Pages: 6, 16)
    symbolic_allele_pattern = "^<(DEL|INS|DUP|INV|CNV|BND)"
    # Ref: https://samtools.github.io/hts-specs/VCFv4.3.pdf (Page: 17)
    complex_rearrangements_breakend_pattern = "^[ATCGNatgcn]+\[.+:.+\[$|^[ATCGNatgcn]+\].+:.+\]$|^\].+:.+\][ATCGNatgcn]+$|^\[.+:.+\[[ATCGNatgcn]+$"
    # Ref: https://samtools.github.io/hts-specs/VCFv4.3.pdf (Page: 18)
    complex_rearrangements_special_breakend_pattern = "^[ATGCNatgcn]+<[0-9A-Za-z!#$%&+./:;?@^_|~-][0-9A-Za-z!#$%&*+./:;=?@^_|~-]*>$"
    # Ref: https://samtools.github.io/hts-specs/VCFv4.3.pdf (Page: 22)
    single_breakend_pattern = "^\.[ATGCNatgcn]+|[ATGCNatgcn]+\.$"
    sv_regex = re.compile(f'{symbolic_allele_pattern}|{complex_rearrangements_breakend_pattern}|'
                          f'{complex_rearrangements_special_breakend_pattern}|{single_breakend_pattern}')
    nb_sv = 0
    if vcf_file.endswith('.gz'):
        ctx = gzip.open(vcf_file, mode="rb")
    else:
        ctx = open(vcf_file, mode="r")
    has_sv_in_vcf = False
    with ctx as open_input, open(output_vcf, 'w') as open_output:
        for line in open_input:
            if line.startswith("#"):
                open_output.write(line)
            sp_line = line.split('\t')
            alt_allele_column = sp_line[4]
            alternate_alleles = alt_allele_column.split(",")
            for alternate_allele in alternate_alleles:
                if re.search(sv_regex, alternate_allele):
                    open_output.write(line)
                    nb_sv += 1
                    has_sv_in_vcf = True
                    break
    if has_sv_in_vcf:
        print(f'{nb_sv} lines containing structural variants')


def main():
    argparse = ArgumentParser(description='Detect and extract the variant line that contains structural variant')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    argparse.add_argument('--vcf_file', required=True, type=str,
                          help='Path to VCF where the detection of SV should be performed')
    argparse.add_argument('--output_vcf_file_with_sv', required=True, type=str,
                          help='Path to VCF where the detected SVs will be output')

    args = argparse.parse_args()
    detect_structural_variant(vcf_file=args.vcf_file, output_vcf=args.output_vcf_file_with_sv)


if __name__ == "__main__":
    main()


