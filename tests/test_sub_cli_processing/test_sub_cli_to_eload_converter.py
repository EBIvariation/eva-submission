import copy
import glob
import hashlib
import json
import tempfile
import os
import shutil
from unittest import TestCase, mock
from unittest.mock import patch, PropertyMock, Mock

from ebi_eva_common_pyutils.config import cfg

from eva_sub_cli_processing.sub_cli_to_submission_converter.sub_cli_to_submission_converter import SubCLIToSubmissionConverter
from eva_submission import ROOT_DIR
from eva_submission.submission_config import load_config, SubmissionConfig
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxWriter


def touch(filepath, content=None):
    with open(filepath, 'w') as open_file:
        if content:
            open_file.write(content)

submission_json = 'submission_ws_response.json'


class TestSubCliToSubmissionConverter(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        webservice_response_file = os.path.join(self.resources_folder, 'submission_ws_response.json')
        with open(webservice_response_file) as open_file:
            self.webservice_response_json = json.load(open_file)
        metadata_json_file = os.path.join(self.resources_folder, 'input_json_for_json_to_xlsx_converter.json')
        with open(metadata_json_file) as open_file:
            self.webservice_response_json['metadataJson'] = json.load(open_file)
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(ROOT_DIR)
        self.submission_id = "submission_1"

        self.cli_to_eload = SubCLIToSubmissionConverter(1, submission_id=self.submission_id)
        self.upload_dir = os.path.join(self.resources_folder, 'upload_dir')
        os.makedirs(self.upload_dir, exist_ok=True)


    def tearDown(self) -> None:
        SubmissionConfig.content = {}
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', self.submission_id))
        for eload in eloads:
            shutil.rmtree(eload)
        nobackup_eloads = glob.glob(os.path.join(self.resources_folder, 'nobackup_eloads', self.submission_id))
        for nobackup_eload in nobackup_eloads:
            shutil.rmtree(nobackup_eload)
        genomes = glob.glob(os.path.join(self.resources_folder, 'genomes'))
        for genome in genomes:
            shutil.rmtree(genome)
        if os.path.exists(self.upload_dir):
            shutil.rmtree(self.upload_dir)

    def create_vcfs(self, num_files=2):
        paths = []
        for i in range(num_files):
            vcf = os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'vcf_files', f'file{i}.vcf')
            touch(vcf)
            paths.append(vcf)
        return paths

    def create_metadata(self, num_analyses=0):
        source_metadata = os.path.join(self.resources_folder, 'metadata.xlsx')
        metadata = os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'metadata_file',
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

    def patch_submission_obj(self, json_response=None):
        if json_response is None:
            json_response =self.webservice_response_json
        m_submission_obj = patch.object(SubCLIToSubmissionConverter, '_submission_obj',
                                        new_callable=PropertyMock(return_value=json_response))
        return m_submission_obj

    def test_check_status(self):
        with self.patch_submission_obj() as m_submission_obj:
            self.cli_to_eload.check_status()
            # Check that nothing was raised
            assert True

    def test_check_status_fail(self):
        with self.patch_submission_obj(json_response={'submission':{'status': "OPEN"}}) as m_submission_obj:
            with self.assertRaises(AssertionError):
                self.cli_to_eload.check_status()

    def test_retrieve_vcf_files_from_sub_cli_ftp(self):
        assert os.listdir(os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'vcf_files')) == []
        assert os.listdir(os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'metadata_file')) == []
        with self.patch_submission_obj() as m_submission_obj:
            self.cli_to_eload.retrieve_vcf_files_from_sub_cli_ftp_dir()
        assert os.listdir(os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'vcf_files')) == ['data.vcf.gz']

    def _upload_dir_with_parts(self, content, part_size):
        """Populate upload directory with content split into parts, and return the parts' description."""
        parts = []
        for index, offset in enumerate(range(0, len(content), part_size)):
            part_name = f'large.vcf.gz.part{index + 1:03d}'
            with open(os.path.join(self.upload_dir, part_name), 'wb') as open_file:
                open_file.write(content[offset:offset + part_size])
            parts.append({'fileName': part_name, 'fileSize': len(content[offset:offset + part_size])})
        return self.upload_dir, parts

    def _submission_with_file_in_parts(self, content, parts, md5=None):
        json_response = copy.deepcopy(self.webservice_response_json)
        json_response['metadataJson']['files'] = [{
            'analysisAlias': 'VD1', 'fileName': 'large.vcf.gz', 'fileSize': len(content),
            'md5': md5 or hashlib.md5(content).hexdigest(), 'fileParts': parts
        }]
        return json_response

    def _assemble(self, upload_dir, json_response):
        vcf_dir = os.path.join(self.cli_to_eload.eload_dir, '10_submitted', 'vcf_files')
        with self.patch_submission_obj(json_response), \
                patch.object(SubCLIToEloadConverter, 'sub_cli_submission_dir_path',
                             new_callable=PropertyMock(return_value=upload_dir)):
            self.cli_to_eload.retrieve_vcf_files_from_sub_cli_ftp_dir()
        return vcf_dir

    def test_retrieve_vcf_files_assembles_file_uploaded_in_parts(self):
        content = bytes(range(256)) * 4
        upload_dir, parts = self._upload_dir_with_parts(content, part_size=300)
        vcf_dir = self._assemble(upload_dir, self._submission_with_file_in_parts(content, parts))
        # The parts are not copied as VCF files, only the assembled file
        assert os.listdir(vcf_dir) == ['large.vcf.gz']
        with open(os.path.join(vcf_dir, 'large.vcf.gz'), 'rb') as open_file:
            assert open_file.read() == content

    def test_retrieve_vcf_files_part_missing(self):
        content = bytes(range(256)) * 4
        upload_dir, parts = self._upload_dir_with_parts(content, part_size=300)
        os.remove(os.path.join(upload_dir, 'large.vcf.gz.part002'))
        with self.assertRaises(ValueError):
            self._assemble(upload_dir, self._submission_with_file_in_parts(content, parts))

    def test_retrieve_vcf_files_assembled_md5_mismatch(self):
        content = bytes(range(256)) * 4
        upload_dir, parts = self._upload_dir_with_parts(content, part_size=300)
        with self.assertRaises(ValueError):
            self._assemble(upload_dir, self._submission_with_file_in_parts(content, parts, md5='0' * 32))

    def test_download_metadata_json_and_store(self):
        with self.patch_submission_obj():
            self.cli_to_eload.download_metadata_json_and_store()

        # Check if json file was written correctly and converted to xlsx without any error
        metadata_json_file_path = os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'metadata_file',
                                               'metadata_json.json')
        metadata_xlsx_file_path = os.path.join(self.cli_to_eload.submission_dir, '10_submitted', 'metadata_file',
                                               'metadata_xlsx.xlsx')
        assert os.path.exists(metadata_json_file_path)
        with open(metadata_json_file_path) as open_file:
            metadata_json = json.load(open_file)
            assert metadata_json.get('project').get('title') == 'Example Project'
            assert metadata_json.get('project').get('description') == 'An example project for demonstration purposes'

    def test_detect_submitted_metadata(self):
        self.create_vcfs()
        metadata = self.create_metadata()

        self.cli_to_eload.detect_submitted_metadata()
        self.cli_to_eload.check_submitted_filenames()
        # Check that the metadata spreadsheet is in the config file
        assert self.cli_to_eload.submission_cfg.query('submission', 'metadata_spreadsheet') == metadata

    def test_detect_metadata_attributes(self):
        self.create_vcfs()
        metadata = self.create_metadata()
        self.cli_to_eload.submission_cfg.set('submission', 'metadata_spreadsheet', value=metadata)
        with patch('eva_submission.submission_utils.get_scientific_name_from_evapro', return_value=None):
            self.cli_to_eload.detect_metadata_attributes()

        assert self.cli_to_eload.submission_cfg.query('submission', 'project_title') == 'Greatest project ever'
        assert self.cli_to_eload.submission_cfg.query('submission', 'taxonomy_id') == 9606
        assert self.cli_to_eload.submission_cfg.query('submission', 'scientific_name') == 'Homo sapiens'
        assert self.cli_to_eload.submission_cfg.query('submission', 'analyses', 'submission_1_GAE',
                                                 'assembly_accession') == 'GCA_000001405.1'
        vcf_files = self.cli_to_eload.submission_cfg.query('submission', 'analyses', 'submission_1_GAE', 'vcf_files')
        assert len(vcf_files) == 1
        assert '10_submitted/vcf_files/T100.vcf.gz' in vcf_files[0]

    def test_check_submitted_filenames_multiple_analyses(self):
        # create some extra vcf files and analyses
        self.create_vcfs(num_files=5)
        self.create_metadata(num_analyses=2)

        self.cli_to_eload.detect_submitted_metadata()
        with self.assertRaises(ValueError):
            self.cli_to_eload.check_submitted_filenames()

    def test_find_genome_single_sequence(self):
        cfg.content['eutils_api_key'] = None
        self.cli_to_eload.submission_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        # Ensure no other analyses present in the config
        self.cli_to_eload.submission_cfg.set('submission', 'analyses', value={})
        self.cli_to_eload.submission_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                             value='AJ312413.2')

        with mock.patch("eva_submission.submission_preparation.requests.put", return_value=mock.Mock(status_code=200)):
            self.cli_to_eload.find_genome()
            assert self.cli_to_eload.submission_cfg['submission']['analyses']['Analysis alias test']['assembly_fasta'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2.fa'
            assert self.cli_to_eload.submission_cfg['submission']['analyses']['Analysis alias test']['assembly_report'] \
                   == 'tests/resources/genomes/thingy_thingus/AJ312413.2/AJ312413.2_assembly_report.txt'

    def test_contig_alias_db_update(self):
        cfg.content['eutils_api_key'] = None
        # Ensure no other analyses present in the config
        self.cli_to_eload.submission_cfg.set('submission', 'analyses', value={})
        self.cli_to_eload.submission_cfg.set('submission', 'scientific_name', value='Thingy thingus')
        self.cli_to_eload.submission_cfg.set('submission', 'analyses', 'Analysis alias test', 'assembly_accession',
                                             value='GCA_000001405.10')

        with mock.patch("eva_submission.submission_preparation.get_reference_fasta_and_report",
                        return_value=('assembly', 'report')), \
                mock.patch("eva_submission.submission_preparation.requests.put") as mockput:
            self.cli_to_eload.find_genome()

            mockput.assert_called_once_with('host/v1/admin/assemblies/GCA_000001405.10', auth=('user', 'pass'))

    @patch("requests.get")
    def test_submission_obj(self, mock_requests):
        mock_response = mock_requests.return_value
        mock_response.status_code = 200
        mock_response.json=Mock(return_value={'json_property': 'json_value'})

        submission_obj = self.cli_to_eload._submission_obj
        # Check if requests.get was called with the correct URL
        mock_requests.assert_called_once_with(
            f"{cfg['submissions']['webservice']['url']}/admin/submission/{self.submission_id}",
            auth=(cfg['submissions']['webservice']['admin_username'],
                  cfg['submissions']['webservice']['admin_password']))
        assert submission_obj == {'json_property': 'json_value'}

    @patch("requests.put")
    def test_store_submission_id_in_config(self, mock_put):
        self.cli_to_eload.add_submission_id_to_config()
        assert self.submission_id == self.cli_to_eload.submission_cfg.query('submission', 'submission_id')
