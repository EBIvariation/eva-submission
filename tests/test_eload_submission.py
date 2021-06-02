import glob
import os
import shutil
from unittest import TestCase

from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.eload_submission import EloadPreparation
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


def touch(filepath, content=None):
    with open(filepath, 'w') as open_file:
        if content:
            open_file.write(content)


class TestEload(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.eload = EloadPreparation(1)

    def tearDown(self) -> None:
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_1'))
        for eload in eloads:
            shutil.rmtree(eload)
        genomes = glob.glob(os.path.join(self.resources_folder, 'genomes'))
        for genome in genomes:
            shutil.rmtree(genome)

    def test_copy_from_ftp(self):
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files')) == []
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file')) == []
        self.eload.copy_from_ftp(1, 'john')
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files')) == ['data.vcf.gz']
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file')) == ['metadata.xlsx']

    def test_detect_submitted_metadata(self):
        # create the data
        vcf1 = os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files', 'file1.vcf')
        vcf2 = os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files', 'file2.vcf')
        touch(vcf1)
        touch(vcf2)
        metadata = os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file', 'metadata.xlsx')
        touch(metadata)

        self.eload.detect_submitted_vcf()
        # Check that the vcf are in the config file
        assert sorted(self.eload.eload_cfg.query('submission', 'vcf_files')) == [vcf1, vcf2]

        self.eload.detect_submitted_metadata()
        # Check that the metadata spreadsheet is in the config file
        assert self.eload.eload_cfg.query('submission', 'metadata_spreadsheet') == metadata

    def test_replace_values_in_metadata(self):
        source_metadata = os.path.join(self.resources_folder, 'metadata.xlsx')
        metadata = os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file', 'metadata.xlsx')
        shutil.copyfile(source_metadata, metadata)

        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 9606
        assert reader.analysis[0]['Reference'] == 'GCA_000001405.1'
        self.eload.replace_values_in_metadata(taxid=10000, reference_accession='GCA_000009999.9')
        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 10000
        assert reader.analysis[0]['Reference'] == 'GCA_000009999.9'

    def test_find_genome_single_sequence(self):
        cfg.content['eutils_api_key'] = None
        self.eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        self.eload.eload_cfg.set('submission', 'assembly_accession', value='AJ312413.2')
        self.eload.find_genome()
        assert self.eload.eload_cfg['submission']['assembly_fasta'] == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2.fa'
        assert 'assembly_report' not in self.eload.eload_cfg['submission']
