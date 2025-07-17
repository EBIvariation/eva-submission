import os
from unittest import TestCase

from eva_submission.steps.rename_contigs_from_insdc_in_assembly import RenameContigsInAssembly


class TestRenameContigs(TestCase):
    resources = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        assembly_accession = 'GCA_000002945.2'
        input_vcf = os.path.join(self.resources, 'vcf_files', 'vcf_file_ASM294v2.vcf')
        assembly_report_path = os.path.join(self.resources, 'GCA_000002945.2', 'GCA_000002945.2_assembly_report.txt')
        assembly_fasta_path = os.path.join(self.resources, 'GCA_000002945.2', 'GCA_000002945.2.fa')
        self.rename = RenameContigsInAssembly(assembly_accession, assembly_fasta_path, assembly_report_path, [input_vcf])

    def test_required_contigs_from_vcf(self):
        assert self.rename.contigs_found_in_vcf == {'I', 'II', 'III', 'MTR', 'MT'}

    def test_assembly_report_map(self):
        assert self.rename.assembly_report_map == {
            'CU329670.1': 'I', 'NC_003424.3': 'I',
            'CU329671.1': 'II', 'NC_003423.3': 'II',
            'CU329672.1': 'III', 'NC_003421.2': 'III',
            'X54421.1': 'MT', 'NC_001326.1': 'MT',
            'FP565355.1': 'MTR'
        }

    def test_contig_alias_map(self):
        assert self.rename.contig_alias_map == {
            'CU329670.1': 'I', 'NC_003424.3': 'I',
            'CU329671.1': 'II', 'NC_003423.3': 'II',
            'CU329672.1': 'III', 'NC_003421.2': 'III',
            'X54421.1': 'MT', 'NC_001326.1': 'MT'
        }


    def test_rename_genome(self):
        assembly_custom = os.path.join(self.resources, 'GCA_000002945.2', 'GCA_000002945.2_custom.fa')
        self.rename.rewrite_changing_names(assembly_custom)


