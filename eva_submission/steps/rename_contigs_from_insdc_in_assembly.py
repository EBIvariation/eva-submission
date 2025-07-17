# Copyright 2022 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import gzip
from argparse import ArgumentParser
from csv import DictReader, excel_tab

import requests
from cached_property import cached_property
from ebi_eva_common_pyutils.contig_alias.contig_alias import ContigAliasClient
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry


class RenameContigsInAssembly(AppLogger):
    """
    This class renames sequences based on the one provided in a set of VCFs
    """
    def __init__(self, assembly_accession, assembly_fasta_path, assembly_report_path, input_vcfs):
        self.input_vcfs = input_vcfs
        self.assembly_accession = assembly_accession
        self.assembly_fasta_path = assembly_fasta_path
        self.assembly_report_path = assembly_report_path
        self.contig_alias_client = ContigAliasClient()

    @cached_property
    def contigs_found_in_vcf(self):
        """Provides the contigs present in the VCF file"""
        contigs = set()
        for input_vcf in self.input_vcfs:
            if input_vcf.endswith('.gz'):
                vcf_in = gzip.open(input_vcf, mode="rt")
            else:
                vcf_in = open(input_vcf, mode="r")
            for line in vcf_in:
                if line.startswith("#"):
                    continue
                contigs.add(line.split('\t')[0])
        return contigs

    @staticmethod
    def _get_assembly_report(assembly_report):
        """Parse the assembly report and return each row as a dict."""
        headers = None
        with open(assembly_report) as open_file:
            # Parse the assembly report file to find the header then stop
            for line in open_file:
                if line.lower().startswith("# sequence-name") and "sequence-role" in line.lower():
                    headers = line.strip().split('\t')
                    break
            reader = DictReader(open_file, fieldnames=headers, dialect=excel_tab)
            return headers, [record for record in reader]

    @cached_property
    def assembly_report_rows(self):
        """Provides assembly report rows with each row as a dict."""
        headers, rows = self._get_assembly_report(self.assembly_report_path)
        self.assembly_report_headers = headers
        return rows

    @cached_property
    def assembly_report_map(self):
        """
        Dictionary of INSDC or Refseq accession to naming convention used in the VCF constructed based on the assembly report.
        """
        assembly_report_map = {}
        for row in self.assembly_report_rows:
            # Search if the contig name is found in the VCF
            for header in ['# Sequence-Name', 'GenBank-Accn', 'RefSeq-Accn', 'UCSC-style-name']:
                if row[header] in self.contigs_found_in_vcf:
                    if row['GenBank-Accn'] and row['GenBank-Accn'] != 'na':
                        assembly_report_map[row['GenBank-Accn']] = row[header]
                    if row['RefSeq-Accn'] and row['RefSeq-Accn'] != 'na':
                        assembly_report_map[row['RefSeq-Accn']] = row[header]
        return assembly_report_map

    @cached_property
    def contig_alias_map(self):
        """
        Dictionary of INSDC or Refseq accession to naming convention used in the VCF constructed based on the contig alias.
        """
        contig_alias_map_tmp = {}
        for entity in self.contig_alias_client.assembly_contig_iter(self.assembly_accession):
            for naming_convention in ['refseq', 'enaSequenceName', 'genbankSequenceName', 'ucscName']:
                if naming_convention in entity and entity[naming_convention]:
                    contig_alias_map_tmp[entity[naming_convention]] = (entity.get('insdcAccession'), entity.get('refseq'))
        contig_alias_map = {}
        # Reverse the map to get the INSDC to Non-INSDC name found in the VCF files
        for contig in self.contigs_found_in_vcf:
            if contig in contig_alias_map_tmp:
                insdc_acc, refseq_acc = contig_alias_map_tmp[contig]
                if insdc_acc:
                    contig_alias_map[insdc_acc] = contig
                if refseq_acc:
                    contig_alias_map[refseq_acc] = contig
        return contig_alias_map

    def rewrite_changing_names(self, output_fasta):
        """Create a new fasta file with contig names use in the VCF."""
        with open(self.assembly_fasta_path) as open_input, open(output_fasta, 'w') as open_output:
            for line in open_input:
                if line.startswith('>'):
                    contig_name = line.split()[0][1:]
                    assembly_report_name = self.assembly_report_map.get(contig_name)
                    contig_alias_name = self.contig_alias_map.get(contig_name)
                    if assembly_report_name:
                        contig_name = assembly_report_name
                    elif contig_alias_name:
                        contig_name = contig_alias_name
                    line = '>' + contig_name + '\n'
                open_output.write(line)


def main():
    argparse = ArgumentParser(description='Convert a genome file from INSDC accession to the naming convention '
                                          'used in the VCFs')
    argparse.add_argument('--assembly_accession', required=True, type=str,
                          help='The assembly accession of this genome')
    argparse.add_argument('--assembly_fasta', required=True, type=str,
                          help='The path to the fasta file containing the genome sequences')
    argparse.add_argument('--custom_fasta', required=True, type=str,
                          help='The path to the fasta file containing the renamed sequences')
    argparse.add_argument('--assembly_report', required=True, type=str,
                          help='The path to the file containing the assembly report')
    argparse.add_argument('--vcf_files', required=True, type=str, nargs='+',
                          help='Path to one or several VCF files')

    args = argparse.parse_args()
    RenameContigsInAssembly(
        assembly_accession=args.assembly_accession, assembly_fasta_path=args.assembly_fasta,
        assembly_report_path=args.assembly_report, input_vcfs=args.vcf_files
    ).rewrite_changing_names(args.custom_fasta)


if __name__ == "__main__":
    main()
