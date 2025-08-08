import glob
import json
import os
import shutil
from unittest import TestCase, mock

import eva_sub_cli
from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import load_config, EloadConfig
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
        EloadConfig.content = {}
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

    def create_metadata(self, v2=False, num_analyses=0):
        if v2:
            source_metadata = os.path.join(self.resources_folder, 'metadata_v2.xlsx')
        else:
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

    def test_detect_submitted_metadata_json(self):
        self.create_vcfs()
        metadata = self.create_metadata(v2=True)
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.eload.convert_new_spreadsheet_to_json()

        self.eload.detect_metadata_attributes()
        self.eload.check_submitted_filenames()
        # Check that the metadata json is in the config file
        metadata_json = metadata.replace('metadata.xlsx', 'eva_sub_cli_metadata.json')
        assert self.eload.eload_cfg.query('submission', 'metadata_json') == metadata_json

    def test_detect_metadata_attributes(self):
        self.create_vcfs()
        metadata = self.create_metadata()
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.eload.detect_metadata_attributes()

        assert self.eload.eload_cfg.query('submission', 'project_title') == 'Greatest project ever'
        assert self.eload.eload_cfg.query('submission', 'taxonomy_id') == 9606
        assert self.eload.eload_cfg.query('submission', 'scientific_name') == 'Homo sapiens'
        assert self.eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE', 'assembly_accession') == 'GCA_000001405.1'
        vcf_files = self.eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE', 'vcf_files')
        assert len(vcf_files) == 1
        assert '10_submitted/vcf_files/T100.vcf.gz' in vcf_files[0]

    def test_detect_metadata_attributes_from_json(self):
        self.create_vcfs()
        metadata = self.create_metadata(v2=True)
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.eload.convert_new_spreadsheet_to_json()
        self.eload.detect_metadata_attributes_from_json()

        assert self.eload.eload_cfg.query('submission', 'project_title') == 'Greatest project ever'
        assert self.eload.eload_cfg.query('submission', 'taxonomy_id') == 9606
        assert self.eload.eload_cfg.query('submission', 'scientific_name') == 'Homo sapiens'
        assert self.eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE',
                                          'assembly_accession') == 'GCA_000001405.1'
        vcf_files = self.eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE', 'vcf_files')
        assert len(vcf_files) == 1
        assert '10_submitted/vcf_files/T100.vcf.gz' in vcf_files[0]

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
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.eload.replace_values_in_metadata(taxid=10000, reference_accession='GCA_000009999.9')
        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 10000
        assert reader.analysis[0]['Reference'] == 'GCA_000009999.9'

    def test_replace_values_in_metadata_json(self):
        metadata = self.create_metadata(v2=True)
        metadata_json = metadata.replace('metadata.xlsx', 'eva_sub_cli_metadata.json')
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.eload.convert_new_spreadsheet_to_json()
        with open(metadata_json) as f:
            metadata_content = json.load(f)
            assert metadata_content['project']['taxId'] == 9606
            assert metadata_content['analysis'][0]['referenceGenome'] == 'GCA_000001405.1'

        self.eload.replace_values_in_metadata(taxid=10000, reference_accession='GCA_000009999.9')
        with open(metadata_json) as f:
            metadata_content = json.load(f)
            assert metadata_content['project']['taxId'] == 10000
            assert metadata_content['analysis'][0]['referenceGenome'] == 'GCA_000009999.9'

    def test_find_genome_single_sequence(self):
        cfg.content['eutils_api_key'] = None
        self.eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        # Ensure no other analyses present in the config
        self.eload.eload_cfg.set('submission', 'analyses', value={})
        self.eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession', value='AJ312413.2')

        with mock.patch("eva_submission.eload_preparation.requests.put", return_value=mock.Mock(status_code=200)):
            self.eload.find_genome()
            assert self.eload.eload_cfg['submission']['analyses']['Analysis alias test']['assembly_fasta'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2.fa'
            assert self.eload.eload_cfg['submission']['analyses']['Analysis alias test']['assembly_report'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2_assembly_report.txt'

    def test_contig_alias_db_update(self):
        cfg.content['eutils_api_key'] = None
        self.eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        self.eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                 value='GCA_000001405.10')

        with mock.patch("eva_submission.eload_preparation.get_reference_fasta_and_report", return_value=('assembly', 'report')), \
                mock.patch("eva_submission.eload_preparation.requests.put") as mockput:

            self.eload.find_genome()

            mockput.assert_called_once_with('host/v1/admin/assemblies/GCA_000001405.10', auth=('user', 'pass'))

    def test_convert_new_spreadsheet_to_json(self):
        metadata_example = os.path.join(eva_sub_cli.ETC_DIR , 'EVA_Submission_Example.xlsx')
        metadata_dir = self.eload._get_dir('metadata')
        # Make a copy so we preserve the example spreadsheet
        metadata_copy = shutil.copy(metadata_example, os.path.join(metadata_dir, 'metadata.xlsx'))
        self.eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata_copy)

        self.eload.convert_new_spreadsheet_to_json()
        assert os.path.isfile(os.path.join(metadata_dir, 'eva_sub_cli', os.path.basename(metadata_copy)))
        json_file = os.path.join(metadata_dir, 'eva_sub_cli_metadata.json')
        assert os.path.isfile(json_file)
        with open(json_file, 'r') as f:
            json_data = json.load(f)
            assert json_data.get('project').get('title') == 'Investigation of human genetic variants'
            assert json_data.get('analysis')[0].get('analysisTitle') == 'Human genetic variation analysis'
            assert self.eload.eload_cfg.query('submission', 'metadata_json') == json_file

    def test_update_metadata_json_if_required(self):
        json_example = os.path.join(self.resources_folder, 'input_json_for_json_to_xlsx_converter.json')
        metadata_dir = self.eload._get_dir('metadata')
        # Make a copy so we preserve the example json
        json_copy = shutil.copy(json_example, os.path.join(metadata_dir, 'metadata.json'))
        self.eload.eload_cfg.set('submission', 'metadata_json', value=json_copy)

        # fasta and assembly report determined by find_genome
        self.eload.eload_cfg.set('submission', 'analyses', 'ELOAD_1_VD1', 'assembly_fasta', value='GCA_000009999.9_fasta.fa')
        self.eload.eload_cfg.set('submission', 'analyses', 'ELOAD_1_VD1', 'assembly_report', value='GCA_000009999.9_report.txt')
        self.eload.eload_cfg.set('submission', 'analyses', 'ELOAD_1_VD2', 'assembly_fasta', value='GCA_000001111.1_fasta.fa')
        self.eload.eload_cfg.set('submission', 'analyses', 'ELOAD_1_VD2', 'assembly_report', value='GCA_000001111.1_report.txt')

        # assert initial state of metadata
        with open(json_copy) as open_json:
            original_metadata = json.load(open_json)
        assert original_metadata['project']['taxId'] == 9606
        assert original_metadata['analysis'][0]['referenceFasta'] == 'GCA_000001405.27_fasta.fa'
        assert 'assemblyReport' not in original_metadata['analysis'][0]
        assert original_metadata['analysis'][1]['referenceFasta'] == 'GCA_000001405.27_fasta.fa'
        assert 'assemblyReport' not in original_metadata['analysis'][1]
        assert original_metadata['analysis'][2]['referenceFasta'] == 'GCA_000001405.27_fasta.fa'
        assert 'assemblyReport' not in original_metadata['analysis'][2]
        assert original_metadata['files'][0]['fileName'] == 'example1.vcf.gz'

        self.eload.update_metadata_json_if_required()

        # assert updated metadata
        with open(json_copy) as open_json:
            updated_metadata = json.load(open_json)
        assert updated_metadata['analysis'][0]['referenceFasta'] == 'GCA_000009999.9_fasta.fa'
        assert updated_metadata['analysis'][0]['assemblyReport'] == 'GCA_000009999.9_report.txt'
        assert updated_metadata['analysis'][1]['referenceFasta'] == 'GCA_000001111.1_fasta.fa'
        assert updated_metadata['analysis'][1]['assemblyReport'] == 'GCA_000001111.1_report.txt'
        assert updated_metadata['analysis'][2]['referenceFasta'] == 'GCA_000001405.27_fasta.fa'
        assert 'assemblyReport' not in updated_metadata['analysis'][2]
        assert 'ELOAD_1/10_submitted/vcf_files' in updated_metadata['files'][0]['fileName']
