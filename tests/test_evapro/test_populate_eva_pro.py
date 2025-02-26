import datetime
import os.path
from unittest import TestCase
from unittest.mock import patch, PropertyMock, Mock

import pytest
from pysam.bcftools import query
from sqlalchemy import create_engine, select

from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import metadata, SampleInFile, Project, Analysis, Submission, LinkedProject, Platform, \
    Taxonomy
from eva_submission.submission_config import load_config


class TestEvaProjectLoader(TestCase):

    resources_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')

    def setUp(self):
        config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources', 'erapro_config.yaml')
        if os.path.exists(config_file):
            load_config(config_file)
        else:
            print(f'Config file {config_file} is not present. Add the config file to run the tests using ERA')
        self.loader = EvaProjectLoader()

    def patch_evapro_engine(self, engine):
        metadata.create_all(engine)
        return patch.object(self.loader, '_evapro_engine', side_effect=PropertyMock(return_value=engine))

    @pytest.mark.skip(reason='Needs access to ERA database')
    def test_load_project_from_ena(self):
        project = 'PRJEB66443'

        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            # Prepare the platforms that are supposed to be in the database before the load
            platform = Platform(platform='Illumina NextSeq 500', manufacturer='Illumina')
            self.loader.eva_session.add(platform)
            self.loader.eva_session.commit()

            self.loader.load_project_from_ena(project)
            session = self.loader.eva_session

            # Loaded project
            result = session.execute(select(Project)).fetchone()
            project = result.Project
            assert project.project_accession == 'PRJEB66443'

            # Loaded Project Link
            result = session.execute(select(LinkedProject)).fetchone()
            linked_project = result.LinkedProject
            assert (
                       linked_project.project_accession,
                       linked_project.linked_project_accession,
                       linked_project.linked_project_relation
                   ) == ('PRJEB66443', 'PRJNA167609', 'PARENT')

            # Loaded Taxonomies
            taxonomies = [
                result.Taxonomy
                for result in session.execute(select(Taxonomy)).fetchall()
            ]
            assert set([(taxonomy.taxonomy_id, taxonomy.common_name, taxonomy.scientific_name, taxonomy.taxonomy_code)
                        for taxonomy in taxonomies]) == {(217634, 'Asian longhorned beetle', 'Anoplophora glabripennis', 'aglabripennis')}

            # Loaded Submissions
            submissions = [
                result.Submission
                for result in session.execute(select(Submission)).fetchall()
            ]
            assert set([(submission.submission_accession, submission.action, submission.type) for submission in
                        submissions]) =={('ERA27275681', 'ADD', 'PROJECT')}

            # Loaded analysis
            analyses = [
                result.Analysis
                for result in session.execute(select(Analysis)).fetchall()
            ]
            assert set([analysis.analysis_accession for analysis in analyses]) == {'ERZ21826811'}
            analysis = analyses[0]
            assert set(p.platform for p in analysis.platforms) == {'Illumina NextSeq 500'}
            assert set(e.experiment_type for e in analysis.experiment_types) == {'Genotyping by sequencing'}
            assert (
                       analysis.assembly_set.taxonomy_id, analysis.assembly_set.assembly_name,
                       analysis.assembly_set.assembly_code
                   ) == (217634, 'Agla_2.0', 'agla20')

    def test_load_project_without_ERA(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        project = 'PRJEB36082'
        project_info = (
            'ERP119220', 'PRJEB36082', 'ERA2336002',
            'Shanghai Jiao Tong University Affiliated Sixth People’s Hospital', 'CTSK', 'Other',
            datetime.datetime(2020, 1, 8, 11, 23, 31),
            'CTSK gene polymorphism', '9606', 'Homo sapiens', None, ''
        )
        submission_info = [
            ('ERA27275681', 'ELOAD_1194', datetime.datetime(2023, 9, 28, 15, 54, 7),
             '2023-10-01', {'type': 'ADD', 'schema': 'project', 'source': 'ELOAD_1194.Project.xml'})
        ]
        analysis_info = [
            ('ERZ498176', 'Identification of a large SNP dataset in Larimichthys crocea', 'Lc-SNP',
             'The large yellow croaker, Larimichthys crocea is a commercially important drum fish (Family: Sciaenidae) native to the East and South China Sea. Habitat deterioration and overfishing have led to significant population decline and the collapse of its fishery over the past decades. In this study, we employed SLAF-seq (specific-locus amplified fragment sequencing) technology to identify single nucleotide polymorphism (SNP) loci across the genome of L. crocea. Sixty samples were selected for SLAF analysis out of 1,000 progeny in the same cohort of a cultured stock. Our analysis obtained a total of 151,253 SLAFs, of which 65.88% (99,652) were identified to be polymorphic, scoring a total of 710,567 SNPs. Further filtration resulted in a final panel of 1,782 SNP loci. The data derived from this work could be beneficial for understanding the genetics of complex phenotypic traits, as well as for developing marker selection-assisted breeding programs in the L. crocea aquaculture.',
             'SEQUENCE_VARIATION', 'Zhejiang Ocean University',
             datetime.datetime(2018, 3, 26, 15, 33, 35),
             'GCA_000972845.1', None, None, {'Whole genome sequencing'}, {'Illumina HiSeq 2500'})
        ]
        samples_info = [
            ('ERS18360856', 'SAMEA115348712'), ('ERS18360857', 'SAMEA115348713'), ('ERS18360858', 'SAMEA115348714')
        ]
        files_info = [
            ('ERZ293539', 'ERF11112570', 'IRIS_313-12319.snp.vcf.gz.tbi', 'b98e6396a38b1658d9e0116692e1dae3', 'TABIX',4),
            ('ERZ293539', 'ERF11112569', 'IRIS_313-12319.snp.vcf.gz', '642b2e31ce4fc6b8c92eb2dc53630d47', 'VCF', 4)
        ]
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            # Prepare the platforms that are supposed to be in the database before the load
            platform = Platform(platform='Illumina HiSeq 2500', manufacturer='Illumina')
            self.loader.eva_session.add(platform)
            self.loader.eva_session.commit()
            # Populate the ENAFinder
            self.loader.ena_project_finder = PropertyMock(
                find_project_from_ena_database=Mock(return_value=project_info),
                find_parent_project=Mock(return_value='PRJNA9558'),
                find_ena_submission=Mock(return_value=submission_info),
                find_analysis_in_ena=Mock(return_value=analysis_info),
                find_samples_in_ena=Mock(return_value=samples_info),
                find_files_in_ena=Mock(return_value=files_info)
            )
            self.loader.load_project_from_ena(project)
            session = self.loader.eva_session
            # Loaded project
            result = session.execute(select(Project)).fetchone()
            project = result.Project
            assert project.project_accession == 'PRJEB36082'

            # Loaded Project Link
            result = session.execute(select(LinkedProject)).fetchone()
            linked_project = result.LinkedProject
            assert (
                linked_project.project_accession,
                linked_project.linked_project_accession,
                linked_project.linked_project_relation
            ) == ('PRJEB36082', 'PRJNA9558', 'PARENT')

            # Loaded Taxonomies
            taxonomies = [
                result.Taxonomy
                for result in session.execute(select(Taxonomy)).fetchall()
            ]
            assert set([(taxonomy.taxonomy_id, taxonomy.common_name, taxonomy.scientific_name,taxonomy.taxonomy_code)
                        for taxonomy in taxonomies]) == {(9606, 'human', 'Homo sapiens', 'hsapiens')}

            # Loaded Submissions
            submissions = [
                result.Submission
                for result in session.execute(select(Submission)).fetchall()
            ]
            assert set([(submission.submission_accession, submission.action, submission.type) for submission in submissions]) == {('ERA27275681', 'ADD', 'PROJECT')}

            # Loaded analysis
            analyses = [
                result.Analysis
                for result in session.execute(select(Analysis)).fetchall()
            ]

            assert set([analysis.analysis_accession for analysis in analyses]) == {'ERZ498176'}
            analysis = analyses[0]
            assert set(p.platform for p in analysis.platforms) == {'Illumina HiSeq 2500'}
            assert set(e.experiment_type for e in analysis.experiment_types) == {'Whole genome sequencing'}
            assert (
                analysis.assembly_set.taxonomy_id, analysis.assembly_set.assembly_name,
                analysis.assembly_set.assembly_code
            ) == (9606, 'L_crocea_1.0', 'lcrocea10')


    def test_load_samples_from_vcf_file(self):
        sample_name_2_sample_accession = {'NA00001': 'SAME000001', 'NA00002':'SAME000002', 'NA00003':'SAME000003'}
        vcf_file = os.path.join(self.resources_dir, 'vcf_files','file_structural_variants.vcf')
        vcf_file_name = os.path.basename(vcf_file)
        vcf_file_md5 = 'md5sum'
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            self.loader.eva_session.begin()
            self.loader.insert_file('prj000001', 1, 1, vcf_file_name,
                                    vcf_file_md5, 'vcf', 'path/to/ftp')
            for sample_name, biosample_accession in sample_name_2_sample_accession.items():
                self.loader.insert_sample(biosample_accession, biosample_accession)
            self.loader.eva_session.commit()

            self.loader.load_samples_from_vcf_file(sample_name_2_sample_accession, vcf_file, vcf_file_md5)
            query = select(SampleInFile)
            expected_results = [
                ('NA00001', 'SAME000001', vcf_file_name),
                ('NA00002', 'SAME000002', vcf_file_name),
                ('NA00003', 'SAME000003', vcf_file_name)
            ]

            results = []
            for result in self.loader.eva_session.execute(query).fetchall():
                sample_in_file = result.SampleInFile
                results.append((sample_in_file.name_in_file, sample_in_file.sample.biosample_accession, sample_in_file.file.filename))
            assert results == expected_results
