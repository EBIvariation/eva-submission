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
        self.rename = RenameContigsInAssembly(assembly_accession, [input_vcf], assembly_fasta_path, assembly_report_path)
    def test_required_contigs_from_vcf(self):
        assert self.rename.contigs_found_in_vcf == {'1'}

    def test_contig_to_rename(self):
        assert self.rename.contig_to_rename == {'I': 'CU329670.1', 'II': 'CU329671.1', 'AB325691.1': 'AB325691.1'}

    def test_rename_genome(self):
        assembly_custom = os.path.join(self.resources, 'GCA_000002945.2', 'GCA_000002945.2_custom.fa')
        self.rename.rewrite_changing_names(assembly_custom)


