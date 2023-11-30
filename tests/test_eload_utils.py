import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from eva_submission.eload_utils import check_existing_project_in_ena, detect_vcf_aggregation, \
    check_project_exists_in_evapro, create_assembly_report_from_fasta
from eva_submission.submission_config import load_config


class TestEloadUtils(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

    def tearDown(self) -> None:
        generated_assembly_report = os.path.join(self.resources_folder, 'GCA_000002945.2',
                                                 'copy_GCA_000002945.2_assembly_report.txt')
        copied_assembly = os.path.join(self.resources_folder, 'GCA_000002945.2', 'copy_GCA_000002945.2.fa')
        for f in [copied_assembly, generated_assembly_report]:
            if os.path.exists(f):
                os.remove(f)

    def test_check_project_exists_in_evapro(self):
        with patch('eva_submission.eload_utils.get_metadata_connection_handle'), \
                patch('eva_submission.eload_utils.get_all_results_for_query', return_value=[('something')]):
            assert check_project_exists_in_evapro('existing project')

        with patch('eva_submission.eload_utils.get_metadata_connection_handle'), \
                patch('eva_submission.eload_utils.get_all_results_for_query', return_value=[]):
            assert not check_project_exists_in_evapro('non existing project')

    def test_check_existing_project_in_ena(self):
        assert check_existing_project_in_ena('PRJ') is False
        assert check_existing_project_in_ena('PRJEB42148') is True

    def test_detect_vcf_aggregation(self):
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_basic_aggregation.vcf')
        ) == 'basic'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_no_aggregation.vcf')
        ) == 'none'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_undetermined_aggregation.vcf')
        ) is None

    def test_create_assembly_report_from_fasta(self):
        fasta_file = os.path.join(self.resources_folder, 'GCA_000002945.2', 'GCA_000002945.2.fa')
        c_fasta_file = os.path.join(self.resources_folder, 'GCA_000002945.2', 'copy_GCA_000002945.2.fa')
        shutil.copy(fasta_file, c_fasta_file)
        report_file = os.path.join(self.resources_folder, 'GCA_000002945.2', 'copy_GCA_000002945.2_assembly_report.txt')
        assert not os.path.isfile(report_file)
        report_file = create_assembly_report_from_fasta(c_fasta_file)
        assert os.path.isfile(report_file)

