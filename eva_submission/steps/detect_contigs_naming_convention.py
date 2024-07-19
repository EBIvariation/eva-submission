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
from collections import defaultdict
from csv import DictReader, excel_tab

import requests
from cached_property import cached_property
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry


class ContigsNamimgConventionChecker(AppLogger):
    """
    This check names of contigs in VCF and report back the naming convention
    """
    def __init__(self, assembly_accession):
        self.assembly_accession = assembly_accession

    def naming_convention_map_for_vcf(self, input_vcf):
        """Provides a set of contigs names present in the VCF file"""
        naming_conventions = defaultdict(list)
        contigs = set
        if input_vcf.endswith('.gz'):
            vcf_in = gzip.open(input_vcf, mode="rt")
        else:
            vcf_in = open(input_vcf, mode="r")
        for line in vcf_in:
            if line.startswith("#"):
                continue
            contigs.add(line.split('\t')[0])
        for contig in contigs:
            naming_conventions[self.contig_convention_map[contig]].append(contig)
        return naming_conventions

    @retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _contig_alias_assembly_get(self, page=0, size=10):
        """queries the contig alias to retrieve the list of chromosome associated with the assembly for one page"""
        url = (f'https://www.ebi.ac.uk/eva/webservices/contig-alias/v1/assemblies/{self.assembly_accession}/'
               f'chromosomes?page={page}&size={size}')
        response = requests.get(url, headers={'accept': 'application/json'})
        response.raise_for_status()
        response_json = response.json()
        return response_json

    @staticmethod
    def _add_chromosomes_convention_to_map(assembly_data, contig_convention_map_tmp):
        """Add non-INSDC to INSDC accession mapping based on the contig alias response."""
        for entity in assembly_data.get('chromosomeEntities', []):
            for naming_convention in ['insdcAccession', 'refseq', 'enaSequenceName', 'genbankSequenceName', 'ucscName']:
                if naming_convention in entity and entity[naming_convention]:
                    contig_convention_map_tmp[entity[naming_convention]].append(naming_convention)

    @cached_property
    def contig_convention_map(self):
        """
        Dictionary of contig names to naming convention based on the contig alias.
        """
        contig_convention_map_tmp = defaultdict(list)
        page = 0
        size = 1000
        response_json = self._contig_alias_assembly_get(page=page, size=size)
        self._add_chromosomes_convention_to_map(response_json.get('_embedded', {}), contig_convention_map_tmp)
        while 'next' in response_json['_links']:
            page += 1
            response_json = self._contig_alias_assembly_get(page=page, size=size)
            self._add_chromosomes_convention_to_map(response_json.get('_embedded', {}), contig_convention_map_tmp)
        return contig_convention_map_tmp

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
    naming_convention = ContigsNamimgConventionChecker(assembly_accession=args.assembly_accession)
    for input_vcf in args.vcf_files:
        naming_convention_map, contigs = naming_convention.naming_convention_map_for_vcf(input_vcf)

    input_vcfs = args.vcf_files
        .rewrite_changing_names(args.custom_fasta)


if __name__ == "__main__":
    main()
