import glob
import os
import shutil
from unittest import TestCase, mock

from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter


def touch(filepath, content=None):
    with open(filepath, 'w') as open_file:
        if content:
            open_file.write(content)


class TestEloadPreparation(TestCase):
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

    def create_vcfs(self, num_files=2):
        paths = []
        for i in range(num_files):
            vcf = os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files', f'file{i}.vcf')
            touch(vcf)
            paths.append(vcf)
        return paths

    def create_metadata(self, num_analyses=0):
        source_metadata = os.path.join(self.resources_folder, 'metadata.xlsx')
        metadata = os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file', 'metadata.xlsx')
        shutil.copyfile(source_metadata, metadata)
        if num_analyses:
            writer = EvaXlsxWriter(metadata)
            writer.set_analysis([
                {
                    header: f'analysis{i}'
                    for header in
                    ['Analysis Title', 'Analysis Alias', 'Description', 'Project Title', 'Experiment Type', 'Reference']
                } for i in range(num_analyses)
            ])
            writer.save()
        return metadata

    def test_copy_from_ftp(self):
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files')) == []
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file')) == []
        self.eload.copy_from_ftp(1, 'john')
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'vcf_files')) == ['data.vcf.gz']
        assert os.listdir(os.path.join(self.eload.eload_dir, '10_submitted', 'metadata_file')) == ['metadata.xlsx']

    def test_detect_submitted_metadata(self):
        self.create_vcfs()
        metadata = self.create_metadata()

        self.eload.detect_submitted_metadata()
        self.eload.check_submitted_filenames()
        # Check that the metadata spreadsheet is in the config file
        assert self.eload.eload_cfg.query('submission', 'metadata_spreadsheet') == metadata

    def test_check_submitted_filenames_multiple_analyses(self):
        # create some extra vcf files and analyses
        vcfs = self.create_vcfs(num_files=5)
        self.create_metadata(num_analyses=2)

        self.eload.detect_submitted_metadata()
        with self.assertRaises(ValueError):
            self.eload.check_submitted_filenames()

    def test_replace_values_in_metadata(self):
        metadata = self.create_metadata()

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
        self.eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession', value='AJ312413.2')

        with mock.patch("eva_submission.eload_preparation.requests.put", return_value=mock.Mock(status_code=200)):
            self.eload.find_genome()
            assert self.eload.eload_cfg['submission']['analyses']['Analysis alias test']['assembly_fasta'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2.fa'
            assert 'assembly_report' not in self.eload.eload_cfg['submission']

    def test_contig_alias_db_update(self):

        cfg.content['eutils_api_key'] = None
        self.eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        self.eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                                   value='GCA_000001405.10')

        with mock.patch("eva_submission.eload_preparation.get_reference_fasta_and_report", return_value=('assembly', 'report')), \
                mock.patch("eva_submission.eload_preparation.requests.put") as mockput:

            self.eload.find_genome()

            mockput.assert_called_once_with('host/v1/admin/assemblies/GCA_000001405.10', auth=('user', 'pass'))



