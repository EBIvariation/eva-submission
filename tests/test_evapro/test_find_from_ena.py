import datetime
import os.path
import unittest

import pytest

from eva_submission.evapro.find_from_ena import ApiEnaProjectFinder
from eva_submission.evapro.populate_evapro import OracleEnaProjectFinder
from eva_submission.submission_config import load_config

@pytest.mark.skip(reason='Needs access to ERA database')
class TestEnaProjectFinder(unittest.TestCase):

    def setUp(self):
        config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources', 'erapro_config.yaml')
        if os.path.exists(config_file):
            load_config(config_file)
        else:
            print(f'Config file {config_file} is not present. Add the config file to run the tests using ERA')
        self.finder = OracleEnaProjectFinder()


    def test_find_project_from_ena_database(self):
        project = 'PRJEB36082'
        result = self.finder.find_project_from_ena_database(project)
        expected_results = (
            'ERP119220', 'PRJEB36082', 'ERA2336002', 'Shanghai Jiao Tong University Affiliated Sixth People’s Hospital',
            'CTSK', 'Other',datetime.datetime(2020, 1, 8, 11, 23, 31),
            'CTSK gene polymorphism',  '9606', 'Homo sapiens', None,
            'Associations of Serum cathepsin K and Polymorphisms in the CTSK Gene With Bone Mineral Density and Markers '
            'of Bone Metabolism in Postmenopausal Chinese Women'
        )
        (study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
        project_title, taxonomy_id, scientific_name, common_name, study_description) = result
        assert result == expected_results

    def test_find_ncbi_project_from_ena_database(self):
        project = 'PRJNA1348893'
        result = self.finder.find_project_from_ena_database(project)
        expected_results = (
            None, 'PRJNA1348893',  None, 'Misr University for Science and Technology (MUST)',
            'PRJNA1348893', None, datetime.datetime(2025, 11, 6, 11, 53, 42),
            'Genetic Analysis in Patients with Chronic Kidney Disease (CKD) and Healthy Controls',
            '9606', 'Homo sapiens', None,
            'This project involves Whole Exome Sequencing (WES) of human samples obtained from individuals diagnosed'
            ' with Chronic Kidney Disease (CKD) and matched healthy control subjects. The goal of the study is to '
            'identify potentially pathogenic genetic variants that may contribute to CKD susceptibility, disease '
            'progression, and clinical phenotype variation. Comparative analysis between the CKD group and healthy '
            'controls enables the detection of disease-associated variants, rare mutations, and disrupted gene pathways.'
        )
        (study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
        project_title, taxonomy_id, scientific_name, common_name, study_description) = result
        assert result == expected_results


    def test_find_parent_project(self):
        project = 'PRJEB36082'
        expected_parent = 'PRJNA9558'
        assert self.finder.find_parent_projects(project) == [expected_parent]

    def test_find_ena_submission(self):
        project = 'PRJEB66443'
        expected_actions = [
            ('ERA27275681', 'ELOAD_1194', datetime.datetime(2023, 9, 28, 15, 54, 7),
             '2023-10-01', {'type': 'ADD', 'schema': 'project', 'source': 'ELOAD_1194.Project.xml'})
        ]
        for project in [project]:
            submission_actions = list(self.finder.find_ena_submission_for_project(project))
            assert submission_actions == expected_actions

    def test_find_ena_submission_no_action(self):
        project = 'PRJEB97324'
        expected_actions = [
            ('ERA34898855', 'ELOAD_1499', datetime.datetime(2025, 9, 10, 15, 6, 54),
             '2025-09-13', {'type': 'ADD'})
        ]
        for project in [project]:
            submission_actions = list(self.finder.find_ena_submission_for_project(project))
            assert submission_actions == expected_actions

    def test_find_analysis_in_ena(self):
        project = 'PRJEB25731'
        expected_analysis = [
            ('ERZ498176', 'Identification of a large SNP dataset in Larimichthys crocea', 'Lc-SNP',
             'The large yellow croaker, Larimichthys crocea is a commercially important drum fish (Family: Sciaenidae) native to the East and South China Sea. Habitat deterioration and overfishing have led to significant population decline and the collapse of its fishery over the past decades. In this study, we employed SLAF-seq (specific-locus amplified fragment sequencing) technology to identify single nucleotide polymorphism (SNP) loci across the genome of L. crocea. Sixty samples were selected for SLAF analysis out of 1,000 progeny in the same cohort of a cultured stock. Our analysis obtained a total of 151,253 SLAFs, of which 65.88% (99,652) were identified to be polymorphic, scoring a total of 710,567 SNPs. Further filtration resulted in a final panel of 1,782 SNP loci. The data derived from this work could be beneficial for understanding the genetics of complex phenotypic traits, as well as for developing marker selection-assisted breeding programs in the L. crocea aquaculture.',
             'SEQUENCE_VARIATION', 'Zhejiang Ocean University',
             datetime.datetime(2018, 3, 26, 15, 33, 35),
             'GCF_000972845.1', None, None, {'Whole genome sequencing'}, {'Illumina HiSeq 2500'})
        ]
        results = list(self.finder.find_analysis_in_ena(project))
        assert results == expected_analysis

    def test_find_analysis_in_ena_from_ncbi_project(self):
        project = 'PRJNA1348893'
        expected_analysis = [
            ('ERZ28775366', 'Whole exome variant calling in CKD  sample', 'ELOAD_1529_CKD_WES_Analysis_01',
             'Variant and CNV analysis of WES data from CKD sample (GRCh37 reference)',
             'SEQUENCE_VARIATION', 'agiomix labs',
             datetime.datetime(2025, 12, 19, 12, 18, 43),
             'GCA_000001405.1', None, None, {'Exome sequencing'}, {'Illumina NovaSeq 6000'}),
            ('ERZ28775367', 'Whole exome variant calling in CKD sample', 'ELOAD_1529_CKD_WES_Analysis_02',
             'Variant and CNV analysis of WES data from CKD sample (GRCh37 reference)', 'SEQUENCE_VARIATION',
             'agiomix labs', datetime.datetime(2025, 12, 19, 12, 18, 43),
             'GCA_000001405.1', None, None, {'Exome sequencing'}, {'Illumina NovaSeq 6000'}),
            ('ERZ28775368', 'Whole exome variant calling in  Control sample', 'ELOAD_1529_CKD_WES_Analysis_03',
             'Variant and CNV analysis of WES data from control sample (GRCh37 reference)', 'SEQUENCE_VARIATION',
             'agiomix labs', datetime.datetime(2025, 12, 19, 12, 18, 44),
             'GCA_000001405.1', None, None, {'Exome sequencing'}, {'Illumina NovaSeq 6000'}),
            ('ERZ28775369', 'Whole exome variant calling in  Control sample', 'ELOAD_1529_CKD_WES_Analysis_04',
             'Variant and CNV analysis of WES data from control sample (GRCh37 reference)', 'SEQUENCE_VARIATION',
             'agiomix labs', datetime.datetime(2025, 12, 19, 12, 18, 44),
             'GCA_000001405.1', None, None, {'Exome sequencing'}, {'Illumina NovaSeq 6000'})
        ]

        results = list(self.finder.find_analysis_in_ena(project))
        assert results == expected_analysis

    def test_find_samples_in_ena(self):
        analysis = 'ERZ23510811'
        expected_samples = [
            ('ERS18360856', 'SAMEA115348712'), ('ERS18360857', 'SAMEA115348713'), ('ERS18360858', 'SAMEA115348714'),
            ('ERS18360859', 'SAMEA115348715'), ('ERS18360860', 'SAMEA115348716'), ('ERS18360861', 'SAMEA115348717'),
            ('ERS18360862', 'SAMEA115348718'), ('ERS18360863', 'SAMEA115348719'), ('ERS18360864', 'SAMEA115348720'),
            ('ERS18360865', 'SAMEA115348721'), ('ERS18360866', 'SAMEA115348722'), ('ERS18360867', 'SAMEA115348723'),
            ('ERS18360868', 'SAMEA115348724'), ('ERS18360869', 'SAMEA115348725'), ('ERS18360870', 'SAMEA115348726'),
            ('ERS18360871', 'SAMEA115348727'), ('ERS18360872', 'SAMEA115348728'), ('ERS18360873', 'SAMEA115348729'),
            ('ERS18360874', 'SAMEA115348730'), ('ERS18360875', 'SAMEA115348731'), ('ERS18360876', 'SAMEA115348732'),
            ('ERS18360877', 'SAMEA115348733'), ('ERS18360878', 'SAMEA115348734'), ('ERS18360879', 'SAMEA115348735'),
            ('ERS18360880', 'SAMEA115348736'), ('ERS18360881', 'SAMEA115348737'), ('ERS18360882', 'SAMEA115348738'),
            ('ERS18360883', 'SAMEA115348739'), ('ERS18360884', 'SAMEA115348740'), ('ERS18360885', 'SAMEA115348741')
        ]
        results = list(self.finder.find_samples_in_ena(analysis))
        assert results == expected_samples


    def test_find_files_in_ena(self):
        analysis = 'ERZ293539'
        results = list(self.finder.find_files_in_ena(analysis))
        expected_files = [
            ('ERZ293539', 'ERF11112570', 'IRIS_313-12319.snp.vcf.gz.tbi', 'b98e6396a38b1658d9e0116692e1dae3', 'TABIX', 4),
            ('ERZ293539', 'ERF11112569', 'IRIS_313-12319.snp.vcf.gz', '642b2e31ce4fc6b8c92eb2dc53630d47', 'VCF', 4)
        ]
        assert results == expected_files


class TestApiEnaProjectFinder(unittest.TestCase):

    def test_find_samples_from_analysis(self):
        analysis = 'ERZ23510811'
        expected_samples_per_analysis = [
            ('SAMEA115348715', '37_U8681540996_TP53'),
            ('SAMEA115348717', '39_B2253900996_TP53'),
            ('SAMEA115348723', 'Sample 16'),
            ('SAMEA115348730', 'Sample 23'),
            ('SAMEA115348731', 'Sample 25'),
            ('SAMEA115348732', 'Sample 26'),
            ('SAMEA115348733', 'Sample 27'),
            ('SAMEA115348713', '35_U8488600996_TP53'),
            ('SAMEA115348716', '38_U8996740996_TP53'),
            ('SAMEA115348719', '41_U9442170996_TP53'),
            ('SAMEA115348721', '43_U9834740996_TP53'),
            ('SAMEA115348722', '44_U9894200996_TP53'),
            ('SAMEA115348724', 'Sample 17'),
            ('SAMEA115348726', 'Sample 19'),
            ('SAMEA115348727', 'Sample 20'),
            ('SAMEA115348729', 'Sample 22'),
            ('SAMEA115348734', 'Sample 28'),
            ('SAMEA115348736', '59_U9252310996_TP53'),
            ('SAMEA115348737', '60_U9244200996_TP53'),
            ('SAMEA115348712', '34_U8318640996_TP53'),
            ('SAMEA115348714', '36_U8696100996_TP53'),
            ('SAMEA115348718', '40_U9275870996_TP53'),
            ('SAMEA115348720', '42_U9453150996_TP53'),
            ('SAMEA115348725', 'Sample 18'),
            ('SAMEA115348728', 'Sample 21'),
            ('SAMEA115348735', '58_U9145960996_TP53'),
            ('SAMEA115348738', '61_U9266520996_TP53'),
            ('SAMEA115348739', '62_U9458180996_TP53'),
            ('SAMEA115348740', '63_U9490010996_TP53'),
            ('SAMEA115348741', '64_U9851770996_TP53')
        ]
        project_finder = ApiEnaProjectFinder()
        samples_for_analysis = project_finder.find_samples_from_analysis(analysis).get(analysis)
        assert sorted(samples_for_analysis) == sorted(expected_samples_per_analysis)

        # project with no samples
        project = 'PRJEB74935'
        expected_samples_per_analysis = {'ERZ23877850': []}
        samples_per_analysis = project_finder.find_samples_from_analysis(project)
        assert samples_per_analysis == expected_samples_per_analysis

    def test_find_samples_from_analysis_xml(self):
        api_finder = ApiEnaProjectFinder()
        analysis_accession = 'ERZ23510811'
        # Sometimes record in the XML and in the file reports return the same results
        assert sorted(api_finder.find_samples_from_analysis_xml(analysis_accession).get(analysis_accession)) ==\
               sorted(api_finder.find_samples_from_analysis(analysis_accession).get(analysis_accession))
        analysis_accession = 'ERZ26111426'
        # Sometimes they DO NOT (and the XML is correct)
        assert sorted(api_finder.find_samples_from_analysis_xml(analysis_accession).get(analysis_accession)) != \
               sorted(api_finder.find_samples_from_analysis(analysis_accession).get(analysis_accession))
