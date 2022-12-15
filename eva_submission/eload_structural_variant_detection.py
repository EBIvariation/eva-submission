#!/usr/bin/env python
import csv
import os
import re
import shutil
import subprocess
import gzip

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from eva_vcf_merge.utils import validate_aliases

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_utils import resolve_single_file_path, detect_vcf_aggregation
from eva_submission.samples_checker import compare_spreadsheet_and_vcf
from eva_submission.xlsx.xlsx_validation import EvaXlsxValidator


def _detect_structural_variant(self):
    vcf_files = self._get_vcf_files()
    for vcf_file in vcf_files:
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
        if vcf_file.endswith('.vcf.gz'):
            open_file = gzip.open(vcf_file, mode="rt")
        else:
            open_file = open(vcf_file, mode="r")
        for file_line in open_file:
            if file_line[0] == "#":
                continue;

            extract_columns = file_line.split("\t")
            alt_allele_column = extract_columns[4]
            alternate_alleles = alt_allele_column.split(",")
            for alternate_allele in alternate_alleles:
                if re.search(sv_regex, alternate_allele):
                    has_sv_per_vcf = True
                    break
            if has_sv_per_vcf:
                break
        open_file.close()


