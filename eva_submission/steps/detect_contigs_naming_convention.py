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
import os.path
from argparse import ArgumentParser
from collections import defaultdict

import yaml
from cached_property import cached_property
from ebi_eva_common_pyutils.contig_alias.contig_alias import ContigAliasClient
from ebi_eva_common_pyutils.logger import AppLogger

# Order in which the naming convention will be kept if multiple are equivalent
naming_convention_priority = {
    'enaSequenceName': 1,
    'genbankSequenceName': 2,
    'ucscName': 3,
    'insdcAccession': 4,
    'refseq': 5
}


class ContigsNamimgConventionChecker(AppLogger):
    """
    This check names of contigs in VCF and report back the naming convention
    """
    def __init__(self, assembly_accession):
        self.assembly_accession = assembly_accession
        self.contig_alias = ContigAliasClient()

    def naming_convention_map_for_vcf(self, input_vcf):
        """Provides a set of contigs names present in the VCF file for each compatible naming convention"""
        naming_convention_map = defaultdict(list)
        if input_vcf.endswith('.gz'):
            vcf_in = gzip.open(input_vcf, mode="rt")
        else:
            vcf_in = open(input_vcf, mode="r")
        for line in vcf_in:
            if line.startswith("#"):
                continue
            contig_name = line.split('\t')[0]
            naming_convention_map[self.get_contig_convention(contig_name)].append(contig_name)
        return dict(naming_convention_map)

    @cached_property
    def _contig_conventions_map(self):
        """
        Dictionary of contig names to naming convention based on the contig alias.
        """
        contig_conventions_map_tmp = defaultdict(list)
        for entity in self.contig_alias.assembly_contig_iter(self.assembly_accession):
            for naming_convention in naming_convention_priority.keys():
                if naming_convention in entity and entity[naming_convention]:
                    contig_conventions_map_tmp[entity[naming_convention]].append(naming_convention)
        return contig_conventions_map_tmp

    def get_contig_convention(self, contig_name):
        naming_conventions = self._contig_conventions_map.get(contig_name)
        if not naming_conventions:
            return 'Not found'
        # prioritise naming conventions and take the highest priority one
        return sorted(naming_conventions, key=lambda nc: naming_convention_priority.get(nc))[0]

    def write_convention_map_to_yaml(self, vcf_files, output_yaml):
        results = []
        for input_vcf in vcf_files:
            naming_convention_to_contigs = self.naming_convention_map_for_vcf(input_vcf)
            if len(naming_convention_to_contigs) == 1:
                naming_convention = list(naming_convention_to_contigs)[0]
                naming_convention_map = None
            else:
                naming_convention = None
                naming_convention_map = naming_convention_to_contigs
            results.append({
                'vcf_file': os.path.basename(input_vcf),
                'naming_convention': naming_convention,
                'naming_convention_map': naming_convention_map,
                'assembly_accession': self.assembly_accession
            })
        with open(output_yaml, 'w') as open_output:
            yaml.safe_dump(results, open_output)


def main():
    argparse = ArgumentParser(description='Convert a genome file from INSDC accession to the naming convention '
                                          'used in the VCFs')
    argparse.add_argument('--assembly_accession', required=True, type=str,
                          help='The assembly accession of this genome')
    argparse.add_argument('--vcf_files', required=True, type=str, nargs='+',
                          help='Path to one or several VCF files')
    argparse.add_argument('--output_yaml', required=True, type=str,
                          help='Path to output_file where the results will be added.')

    args = argparse.parse_args()
    naming_convention = ContigsNamimgConventionChecker(assembly_accession=args.assembly_accession)
    naming_convention.write_convention_map_to_yaml(args.vcf_files, args.output_yaml)


if __name__ == "__main__":
    main()
