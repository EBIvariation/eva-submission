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
import os
from argparse import ArgumentParser
from csv import DictReader, excel_tab

import requests
from cached_property import cached_property
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry


class RenameContigsInAssembly(AppLogger):
    """
    This class renames all the sequence using the specified mapping.
    """
    def __init__(self, assembly_accession, assembly_fasta_path, assembly_report_path, input_vcfs):
        self.input_vcfs = input_vcfs
        self.assembly_accession = assembly_accession
        self.assembly_fasta_path = assembly_fasta_path
        self.assembly_report_path = assembly_report_path

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

    def rewrite_changing_names(self, output_fasta):
        with open(self.assembly_fasta_path) as open_input, open(output_fasta, 'w') as open_output:
            for line in open_input:
                if line.startswith('>'):
                    contig_name = line.split()[0][1:]
                    new_name = self.assembly_report_map.get(contig_name)
                    new_name2 = self.contig_alias_map.get(contig_name)
                    print(contig_name, new_name, new_name2)
                    if new_name:
                        contig_name = new_name
                    line = '>' + contig_name + '\n'
                open_output.write(line)

    @cached_property
    def assembly_report_map(self):
        assembly_report_map = {}
        for row in self.assembly_report_rows:
            # Search if the contig name is found in the VCF
            for header in ['# Sequence-Name', 'GenBank-Accn', 'RefSeq-Accn', 'UCSC-style-name']:
                if row[header] in self.contigs_found_in_vcf:
                    assembly_report_map[row['GenBank-Accn']] = row[header]
        return assembly_report_map

    @retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _assembly_get(self, page=0, size=10):
        url = (f'https://www.ebi.ac.uk/eva/webservices/contig-alias/v1/assemblies/{self.assembly_accession}/'
               f'chromosomes?page={page}&size={size}')
        response = requests.get(url, headers={'accept': 'application/json'})
        response.raise_for_status()
        response_json = response.json()
        return response_json

    @staticmethod
    def _add_chromosomes_to_map(assembly_data, contig_alias_map):
        for entity in assembly_data.get('chromosomeEntities', []):
            for naming_convention in ['refseq', 'enaSequenceName', 'genbankSequenceName', 'ucscName']:
                if naming_convention in entity and entity[naming_convention]:
                    contig_alias_map[entity[naming_convention]] = entity['insdcAccession']

    @cached_property
    def contig_alias_map(self):
        contig_alias_map_tmp = {}
        page = 0
        size = 1000
        response_json = self._assembly_get(page=page, size=size)
        self._add_chromosomes_to_map(response_json['_embedded'], contig_alias_map_tmp)
        while 'next' in response_json['_links']:
            page += 1
            response_json = self._assembly_get(page=page, size=size)
            self._add_chromosomes_to_map(response_json['_embedded'], contig_alias_map_tmp)
        contig_alias_map = {}
        for contig in self.contigs_found_in_vcf:
            if contig in contig_alias_map_tmp:
                contig_alias_map[contig_alias_map_tmp[contig]] = contig
        return contig_alias_map

    @cached_property
    def contig_to_rename(self):
        rename_map = {}
        for row in self.assembly_report_rows:
            for header in ['# Sequence-Name', 'RefSeq-Accn', 'UCSC-style-name']:
                if row[header] in self.contigs_found_in_vcf:
                    rename_map[row[header]] = row['GenBank-Accn']
        assert self.contigs_found_in_vcf == set(rename_map), \
            f'There are {len(self.contigs_found_in_vcf - set(rename_map))} contigs missing from the translation map'
        return rename_map


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
    rename = RenameContigsInAssembly(assembly_accession=args.assembly_accession, assembly_fasta_path=args.assembly_fasta,
                            assembly_report_path=args.assembly_report, input_vcfs=args.vcf_files)
    rename.rewrite_changing_names(args.custom_fasta)


if __name__ == "__main__":
    main()