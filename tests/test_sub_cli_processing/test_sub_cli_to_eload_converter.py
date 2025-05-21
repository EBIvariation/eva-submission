import glob
import json
import os
import shutil
from unittest import TestCase, mock
from unittest.mock import patch

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_to_eload_converter.sub_cli_to_eload_converter import SubCLIToEloadConverter
from eva_submission import ROOT_DIR
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxWriter, EvaXlsxReader


def touch(filepath, content=None):
    with open(filepath, 'w') as open_file:
        if content:
            open_file.write(content)


class TestSubCliToEloadConverter(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.cli_to_eload = SubCLIToEloadConverter(1)

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
            vcf = os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'vcf_files', f'file{i}.vcf')
            touch(vcf)
            paths.append(vcf)
        return paths

    def create_metadata(self, num_analyses=0):
        source_metadata = os.path.join(self.resources_folder, 'metadata.xlsx')
        metadata = os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'metadata_file',
                                'metadata.xlsx')
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

    def test_retrieve_vcf_files_from_sub_cli_ftp(self):
        assert os.listdir(os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'vcf_files')) == []
        assert os.listdir(os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'metadata_file')) == []
        self.cli_to_eload.retrieve_vcf_files_from_sub_cli_ftp_dir('webin123_webin', 'abcdef_ghijkl_mnopqr_stuvwx')
        assert os.listdir(os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'vcf_files')) == ['data.vcf.gz']

    @patch("requests.get")
    def test_download_metadata_json_and_convert_to_xlsx(self, mock_requests):
        mock_response = mock_requests.return_value
        mock_response.status_code = 200
        input_json_file = os.path.join(self.resources_folder, 'input_json_for_json_to_xlsx_converter.json')
        with open(input_json_file) as json_file:
            input_json_data = json.load(json_file)
        mock_response.json.return_value = {"metadataJson": input_json_data}

        submission_id = "submission123"
        self.cli_to_eload.download_metadata_json_and_convert_to_xlsx(submission_id)

        # Check if requests.get was called with the correct URL
        mock_requests.assert_called_once_with(
            f"{cfg['submissions']['webservice']['url']}/admin/submission/{submission_id}",
            auth=(cfg['submissions']['webservice']['admin_username'],
                  cfg['submissions']['webservice']['admin_password']))

        # Check if json file was written correctly and converted to xlsx without any error
        metadata_json_file_path = os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'metadata_file',
                                               'metadata_json.json')
        metadata_xlsx_file_path = os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'metadata_file',
                                               'metadata_xlsx.xlsx')
        assert os.path.exists(metadata_json_file_path)
        assert os.path.exists(metadata_xlsx_file_path)
        assert self.cli_to_eload.eload_cfg.query('submission', 'metadata_json') == metadata_json_file_path

    def test_detect_submitted_metadata(self):
        self.create_vcfs()
        metadata = self.create_metadata()

        self.cli_to_eload.detect_submitted_metadata()
        self.cli_to_eload.check_submitted_filenames()
        # Check that the metadata spreadsheet is in the config file
        assert self.cli_to_eload.eload_cfg.query('submission', 'metadata_spreadsheet') == metadata

    def test_detect_metadata_attributes(self):
        self.create_vcfs()
        metadata = self.create_metadata()
        self.cli_to_eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        self.cli_to_eload.detect_metadata_attributes()

        assert self.cli_to_eload.eload_cfg.query('submission', 'project_title') == 'Greatest project ever'
        assert self.cli_to_eload.eload_cfg.query('submission', 'taxonomy_id') == 9606
        assert self.cli_to_eload.eload_cfg.query('submission', 'scientific_name') == 'Homo sapiens'
        assert self.cli_to_eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE',
                                                 'assembly_accession') == 'GCA_000001405.1'
        vcf_files = self.cli_to_eload.eload_cfg.query('submission', 'analyses', 'ELOAD_1_GAE', 'vcf_files')
        assert len(vcf_files) == 1
        assert '10_submitted/vcf_files/T100.vcf.gz' in vcf_files[0]

    def test_check_submitted_filenames_multiple_analyses(self):
        # create some extra vcf files and analyses
        self.create_vcfs(num_files=5)
        self.create_metadata(num_analyses=2)

        self.cli_to_eload.detect_submitted_metadata()
        with self.assertRaises(ValueError):
            self.cli_to_eload.check_submitted_filenames()

    def test_replace_values_in_metadata(self):
        metadata = self.create_metadata()

        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 9606
        assert reader.analysis[0]['Reference'] == 'GCA_000001405.1'
        self.cli_to_eload.replace_values_in_metadata(taxid=10000, reference_accession='GCA_000009999.9')
        reader = EvaXlsxReader(metadata)
        assert reader.project['Tax ID'] == 10000
        assert reader.analysis[0]['Reference'] == 'GCA_000009999.9'

    def test_find_genome_single_sequence(self):
        cfg.content['eutils_api_key'] = None
        self.cli_to_eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        # Ensure no other analyses present in the config
        self.cli_to_eload.eload_cfg.set('submission', 'analyses', value={})
        self.cli_to_eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                        value='AJ312413.2')

        with mock.patch("eva_submission.eload_preparation.requests.put", return_value=mock.Mock(status_code=200)):
            self.cli_to_eload.find_genome()
            assert self.cli_to_eload.eload_cfg['submission']['analyses']['Analysis alias test']['assembly_fasta'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2.fa'
            assert self.cli_to_eload.eload_cfg['submission']['analyses']['Analysis alias test']['assembly_report'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2_assembly_report.txt'

    def test_contig_alias_db_update(self):
        cfg.content['eutils_api_key'] = None
        self.cli_to_eload.eload_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        self.cli_to_eload.eload_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                        value='GCA_000001405.10')

        with mock.patch("eva_submission.eload_preparation.get_reference_fasta_and_report",
                        return_value=('assembly', 'report')), \
                mock.patch("eva_submission.eload_preparation.requests.put") as mockput:
            self.cli_to_eload.find_genome()

            mockput.assert_called_once_with('host/v1/admin/assemblies/GCA_000001405.10', auth=('user', 'pass'))
