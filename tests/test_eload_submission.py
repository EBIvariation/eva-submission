import glob
import os
import shutil
from unittest import TestCase

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

    def test_copy_from_ftp(self):
        eload = EloadPreparation(1)
        assert os.listdir(os.path.join(eload.eload_dir, '10_submitted', 'vcf_files')) == []
        assert os.listdir(os.path.join(eload.eload_dir, '10_submitted', 'metadata_file')) == []
        eload.copy_from_ftp(1, 'john')
        assert os.listdir(os.path.join(eload.eload_dir, '10_submitted', 'vcf_files')) == ['data.vcf.gz']
        assert os.listdir(os.path.join(eload.eload_dir, '10_submitted', 'metadata_file')) == ['metadata.xlsx']

    def test_detect_submitted_metadata(self):
        # create the eload
        eload = EloadPreparation(1)
        # create the data
        vcf1 = os.path.join(eload.eload_dir, '10_submitted', 'vcf_files', 'file1.vcf')
        vcf2 = os.path.join(eload.eload_dir, '10_submitted', 'vcf_files', 'file2.vcf')
        touch(vcf1)
        touch(vcf2)
        metadata = os.path.join(eload.eload_dir, '10_submitted', 'metadata_file', 'metadata.xlsx')
        touch(metadata)

        eload.detect_submitted_vcf()
        # Check that the vcf are in the config file
        assert sorted(eload.eload_cfg.query('submission', 'vcf_files')) == [vcf1, vcf2]

        eload.detect_submitted_metadata()
        # Check that the metadata spreadsheet is in the config file
        assert eload.eload_cfg.query('submission', 'metadata_spreadsheet') == metadata

    def test_replace_values_in_metadata(self):
        # create the eload
        eload = EloadPreparation(1)
        source_metadata = os.path.join(self.resources_folder, 'metadata.xlsx')
        metadata = os.path.join(eload.eload_dir, '10_submitted', 'metadata_file', 'metadata.xlsx')
        shutil.copyfile(source_metadata, metadata)

        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 9606
        assert reader.analysis[0]['Reference'] == 'GCA_000001405.1'
        eload.replace_values_in_metadata(taxid=10000, reference_accession='GCA_000009999.9')
        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 10000
        assert reader.analysis[0]['Reference'] == 'GCA_000009999.9'

