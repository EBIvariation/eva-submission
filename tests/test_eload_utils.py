import os
import shutil
from unittest import TestCase
from unittest.mock import patch, Mock

from lxml import etree

from eva_submission.eload_utils import check_existing_project_in_ena, detect_vcf_aggregation, \
    check_project_exists_in_evapro, create_assembly_report_from_fasta, get_hold_date_from_ena
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
            os.path.join(self.resources_folder, 'vcf_files', 'file_basic_aggregation.vcf.gz')
        ) == 'basic'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_no_aggregation.vcf')
        ) == 'none'
        assert detect_vcf_aggregation(
            os.path.join(self.resources_folder, 'vcf_files', 'file_no_aggregation.vcf.gz')
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

    def test_get_hold_date_from_ena(self):
        expected_receipt_xml_public = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
<RECEIPT receiptDate="2025-09-08T11:30:38.041+01:00" submissionFile="SUBMISSION" success="true">
     <ANALYSIS accession="ERZ279016" alias="ELOAD_82_A16G_analysis_13" status="PUBLIC"/>
     <STUDY accession="ERP014624" alias="Anopheles 16 genomes project - Anopheles epiroticus variant calls" status="PUBLIC">
          <EXT_ID accession="PRJEB13088" type="Project"/>
     </STUDY>
     <PROJECT accession="PRJEB13088" alias="Anopheles 16 genomes project - Anopheles epiroticus variant calls" status="PUBLIC"/>
     <SUBMISSION accession="ERA583656" alias="Anopheles 16 genomes project - Anopheles epiroticus variant calls"/>

     <ACTIONS>RECEIPT</ACTIONS>
</RECEIPT>
        '''
        expected_project_xml_public = '''<PROJECT_SET>
<PROJECT accession="PRJEB13088" alias="Anopheles 16 genomes project - Anopheles epiroticus variant calls" broker_name="European Bioinformatics Institute" center_name="European Bioinformatics Institute">
  <IDENTIFIERS>
    <PRIMARY_ID>PRJEB13088</PRIMARY_ID>
    <SECONDARY_ID>ERP014624</SECONDARY_ID>
    <SUBMITTER_ID namespace="European Bioinformatics Institute">Anopheles 16 genomes project - Anopheles epiroticus variant calls</SUBMITTER_ID>
  </IDENTIFIERS>
  <TITLE>Highly evolvable malaria vectors: The genomes of 16 Anopheles mosquitoes. Anopheles epiroticus samples.</TITLE>
  <DESCRIPTION>Anopheles epiroticus subset of project sequencing</DESCRIPTION>
  <PROJECT_ATTRIBUTES>
    <PROJECT_ATTRIBUTE>
      <TAG>ENA-FIRST-PUBLIC</TAG>
      <VALUE>2016-03-17</VALUE>
    </PROJECT_ATTRIBUTE>
    <PROJECT_ATTRIBUTE>
      <TAG>ENA-LAST-UPDATE</TAG>
      <VALUE>2021-01-08</VALUE>
    </PROJECT_ATTRIBUTE>
  </PROJECT_ATTRIBUTES>
</PROJECT>
</PROJECT_SET>'''
        expected_project_xml_public_as_ET = etree.XML(bytes(expected_project_xml_public, encoding='utf-8'))
        with patch('eva_submission.eload_utils.requests.post', return_value=Mock(status_code=200, text=expected_receipt_xml_public)),\
                patch('eva_submission.eload_utils.download_xml_from_ena', return_value=expected_project_xml_public_as_ET):
            get_hold_date_from_ena(project_accession='PRJEB13088', project_alias='Anopheles 16 genomes project - Anopheles epiroticus variant calls')
