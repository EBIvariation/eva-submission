import os
from unittest import TestCase

import yaml

from eva_submission.steps.detect_contigs_naming_convention import ContigsNamimgConventionChecker


class TestContigsNamimgConventionChecker(TestCase):
    resources = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        self.assembly_accession = 'GCA_000002945.2'
        self.input_vcf = os.path.join(self.resources, 'vcf_files', 'vcf_file_ASM294v2.vcf')
        self.output_yaml = os.path.join(self.resources, 'output_ASM294v2.yaml')
        self.checker = ContigsNamimgConventionChecker(self.assembly_accession)

    def tearDown(self) -> None:
        if os.path.exists(self.output_yaml):
            os.remove(self.output_yaml)

    def test_contig_conventions_map(self):
        expected_map = {
            'CU329670.1': ['insdcAccession'],
            'NC_003424.3': ['refseq'],
            'I': ['enaSequenceName', 'genbankSequenceName'],
            'CU329671.1': ['insdcAccession'],
            'NC_003423.3': ['refseq'],
            'II': ['enaSequenceName', 'genbankSequenceName'],
            'CU329672.1': ['insdcAccession'],
            'NC_003421.2': ['refseq'],
            'III': ['enaSequenceName', 'genbankSequenceName'],
            'X54421.1': ['insdcAccession'],
            'NC_001326.1': ['refseq'],
            'MT': ['enaSequenceName', 'genbankSequenceName']
        }
        all_contigs = dict(self.checker._contig_conventions_map)
        assert all_contigs == expected_map

    def test_get_contig_convention(self):
        assert self.checker.get_contig_convention('MT') == 'enaSequenceName'

    def test_naming_convention_map_for_vcf(self):
        expected_convention = {'enaSequenceName': ['I', 'II', 'III', 'MT'], 'Not found': ['MTR']}
        convention_map = self.checker.naming_convention_map_for_vcf(self.input_vcf)
        assert convention_map == expected_convention

    def test_write_convention_map_to_yaml(self):
        expected_list = [{'assembly_accession': 'GCA_000002945.2', 'naming_convention': None,
         'naming_convention_map': {'Not found': ['MTR'], 'enaSequenceName': ['I', 'II', 'III', 'MT']},
         'vcf_file': os.path.basename(self.input_vcf)}]
        self.checker.write_convention_map_to_yaml([self.input_vcf], self.output_yaml)
        with open(self.output_yaml) as open_yaml:
            data = yaml.safe_load(open_yaml)
            print(data)
            assert data == expected_list