import datetime
import os.path
from datetime import date
from unittest import TestCase
from unittest.mock import patch, PropertyMock, Mock

import pytest
from sqlalchemy import create_engine, select

from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import metadata, SampleInFile, Project, Analysis, Submission, LinkedProject, Platform, \
    Taxonomy, ProjectSampleTemp1, BrowsableFile, File, ClusteredVariantUpdate
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

    def load_project_from_ena_and_assert(self, project_accession, linked_project_info, taxonomies_info_set,
                                         submissions_info_set, analyses_set, platform, experiment_types, assembly_info):
        eload = 101
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            # Prepare the platforms that are supposed to be in the database before the load
            platform_obj = Platform(platform='Illumina NextSeq 500', manufacturer='Illumina')
            self.loader.eva_session.add(platform_obj)
            self.loader.eva_session.commit()

            self.loader.load_project_from_ena(project_accession, eload)
            session = self.loader.eva_session

            # Loaded project
            result = session.execute(select(Project)).fetchone()
            project = result.Project
            assert project.project_accession == project_accession

            if linked_project_info:
                # Loaded Project Link
                result = session.execute(select(LinkedProject)).fetchone()
                linked_project = result.LinkedProject
                assert (
                           linked_project.project_accession,
                           linked_project.linked_project_accession,
                           linked_project.linked_project_relation
                       ) == linked_project_info

            # Loaded Taxonomies
            taxonomies = [
                result.Taxonomy
                for result in session.execute(select(Taxonomy)).fetchall()
            ]
            assert set([(taxonomy.taxonomy_id, taxonomy.common_name, taxonomy.scientific_name, taxonomy.taxonomy_code)
                        for taxonomy in taxonomies]) == taxonomies_info_set

            # Loaded Submissions
            submissions = [
                result.Submission
                for result in session.execute(select(Submission)).fetchall()
            ]
            assert set([(submission.submission_accession, submission.action, submission.type) for submission in
                        submissions]) == submissions_info_set

            # Loaded analysis
            analyses = [
                result.Analysis
                for result in session.execute(select(Analysis)).fetchall()
            ]
            assert set([analysis.analysis_accession for analysis in analyses]) == analyses_set
            analysis = analyses[0]
            if platform:
                assert set(p.platform for p in analysis.platforms).pop() == platform
            assert set(e.experiment_type for e in analysis.experiment_types).pop() == experiment_types
            assert (
                       analysis.assembly_set.taxonomy_id, analysis.assembly_set.assembly_name,
                       analysis.assembly_set.assembly_code
                   ) == assembly_info


    @pytest.mark.skip(reason='Needs access to ERA database')
    def test_load_project_from_ena(self):
        self.load_project_from_ena_and_assert(
            project_accession='PRJEB66443',
            linked_project_info=('PRJEB66443', 'PRJNA167609', 'PARENT'),
            taxonomies_info_set={(217634, 'Asian longhorned beetle', 'Anoplophora glabripennis', 'aglabripennis')},
            submissions_info_set={('ERA27275681', 'ADD', 'PROJECT')},
            analyses_set={'ERZ21826811'},
            platform=None,
            experiment_types='Genotyping by sequencing',
            assembly_info=(217634, 'Agla_2.0', 'agla20')
        )

    @pytest.mark.skip(reason='Needs access to ERA database')
    def test_load_new_project_from_ena(self):
        self.load_project_from_ena_and_assert(
            project_accession='PRJEB97324',
            linked_project_info=None,
            taxonomies_info_set= {(207598, None, 'Homininae', 'h')},
            submissions_info_set={('ERA34898855', 'ADD', 'PROJECT')},
            analyses_set={'ERZ28437207'},
            platform='Illumina NextSeq 500',
            experiment_types='Curation',
            assembly_info=(207598, 'GRCh38', 'grch38')
        )

    def test_load_project_without_ERA(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        project = 'PRJEB36082'
        eload = 101
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
             'GCA_000972845.1', None, None, {'GS00000.1'}, {'Whole genome sequencing'}, {'Illumina HiSeq 2500'})
        ]
        samples_info = [
            ('ERS18360856', 'SAMEA115348712'), ('ERS18360857', 'SAMEA115348713'), ('ERS18360858', 'SAMEA115348714')
        ]
        files_info = [
            ('ERZ293539', 'ERF11112570', 'IRIS_313-12319.snp.vcf.gz.tbi', 'b98e6396a38b1658d9e0116692e1dae3', 'TABIX',
             4),
            ('ERZ293539', 'ERF11112569', 'IRIS_313-12319.snp.vcf.gz', '642b2e31ce4fc6b8c92eb2dc53630d47', 'VCF', 4)
        ]
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            # Prepare the platforms that are supposed to be in the database before the load
            platform = Platform(platform='Illumina HiSeq 2500', manufacturer='Illumina')

            self.loader.eva_session.add(platform)
            # Add a dummy project to initiate eva_study_accession
            project = Project(project_accession='dummy', center_name='dummy', alias='dummy', title='dummy',
                              description='dummy', scope='dummy', material='dummy', type='dummy', study_type='dummy',
                              secondary_study_id='dummy', eva_study_accession=1)
            self.loader.eva_session.add(project)

            self.loader.eva_session.commit()
            # Populate the ENAFinder
            self.loader.ena_project_finder = PropertyMock(
                find_project_from_ena_database=Mock(return_value=project_info),
                find_parent_projects=Mock(return_value=['PRJNA9558']),
                find_ena_submission_for_project=Mock(return_value=submission_info),
                find_analysis_in_ena=Mock(return_value=analysis_info),
                find_samples_in_ena=Mock(return_value=samples_info),
                find_files_in_ena=Mock(return_value=files_info)
            )
            self.loader.load_project_from_ena(project, eload)
            session = self.loader.eva_session
            # Loaded project
            result = session.execute(select(Project).where(Project.project_accession != 'dummy')).fetchone()
            project = result.Project
            assert project.project_accession == 'PRJEB36082'
            assert project.eva_study_accession == 2

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
            assert set([(taxonomy.taxonomy_id, taxonomy.common_name, taxonomy.scientific_name, taxonomy.taxonomy_code)
                        for taxonomy in taxonomies]) == {(9606, 'human', 'Homo sapiens', 'hsapiens')}

            # Loaded Submissions
            submissions = [
                result.Submission
                for result in session.execute(select(Submission)).fetchall()
            ]
            assert set([(submission.submission_accession, submission.action, submission.type) for submission in
                        submissions]) == {('ERA27275681', 'ADD', 'PROJECT')}

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
            assert analysis.sequences[0].sequence_accession == 'GS00000.1'

    def test_load_samples_from_vcf_file(self):
        sample_name_2_sample_accession = {'NA00001': 'SAME000001', 'NA00002': 'SAME000002', 'NA00003': 'SAME000003'}
        vcf_file = os.path.join(self.resources_dir, 'vcf_files', 'file_structural_variants.vcf')
        vcf_file_name = os.path.basename(vcf_file)
        vcf_file_md5 = 'md5sum'
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            self.loader.begin_or_continue_transaction()
            self.loader.insert_file('prj000001', 1, 1, vcf_file_name,
                                    vcf_file_md5, 'vcf', 10, 'path/to/ftp')
            for sample_name, biosample_accession in sample_name_2_sample_accession.items():
                self.loader.insert_sample(biosample_accession, biosample_accession)
            self.loader.eva_session.commit()

            self.loader.load_samples_from_vcf_file(sample_name_2_sample_accession, vcf_file, vcf_file_md5, analysis_accession=None)
            query = select(SampleInFile)
            expected_results = [
                ('NA00001', 'SAME000001', vcf_file_name),
                ('NA00002', 'SAME000002', vcf_file_name),
                ('NA00003', 'SAME000003', vcf_file_name)
            ]

            results = []
            for result in self.loader.eva_session.execute(query).fetchall():
                sample_in_file = result.SampleInFile
                results.append((sample_in_file.name_in_file, sample_in_file.sample.biosample_accession,
                                sample_in_file.file.filename))
            assert results == expected_results

    def test_load_samples_from_analysis(self):
        sample_name_2_sample_accession = {'POP': 'SAME000001', 'not present': 'SAME000002'}
        vcf_file = os.path.join(self.resources_dir, 'vcf_files', 'file_basic_aggregation.vcf')
        vcf_file_name = os.path.basename(vcf_file)
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            self.loader.begin_or_continue_transaction()
            analysis_obj = self.loader.insert_analysis('ERZ000001', 'title', 'alias', 'description', 'center_name',
                                        datetime.datetime(2018, 3, 26, 15, 33, 35), 1)
            file_obj = self.loader.insert_file('prj000001', 1, 1, vcf_file_name,
                                    'md5sum', 'vcf', 10, 'path/to/ftp')
            analysis_obj.files.append(file_obj)
            self.loader.insert_sample('SAME000001', 'SAME000001')
            self.loader.eva_session.commit()

            # Only SAME000001 found for this analysis
            self.loader.ena_project_finder = PropertyMock(
                find_samples_in_ena=Mock(return_value=[('ERS000001', 'SAME000001')])
            )

            self.loader.load_samples_from_analysis(sample_name_2_sample_accession, 'ERZ000001')
            query = select(SampleInFile)
            expected_results = [('POP', 'SAME000001', vcf_file_name)]

            results = []
            for result in self.loader.eva_session.execute(query).fetchall():
                sample_in_file = result.SampleInFile
                results.append((sample_in_file.name_in_file, sample_in_file.sample.biosample_accession,
                                sample_in_file.file.filename))
            assert results == expected_results

    def _load_project_analysis_files_samples(
            self, project_accession='prj000001',  analysis_accession='erz000001',
            vcf_files=['vcf_file1.vcf', 'vcf_file2.vcf'],  vcf_file_md5=['md5sum1', 'md5sum2'],
            biosamples=['SAME000001', 'SAME000002'],
            add_browsable=False
    ):
        self.loader.begin_or_continue_transaction()
        project_obj = self.loader.insert_project_in_evapro(
            project_accession=project_accession, center_name='name', project_alias='alias', title='title',
            description='description', ena_study_type='ena_study_type',
            ena_secondary_study_id='ena_secondary_study_id'
        )
        analysis_obj = self.loader.insert_analysis(
            analysis_accession=analysis_accession, title='title', alias='alais', description='description',
            center_name='name', date=datetime.date(year=2024, month=1, day=1), assembly_set_id=1,
            vcf_reference_accession='GCA000001'
        )
        project_obj.analyses.append(analysis_obj)
        file_objs = []
        sample_objs = []
        for vcf_file, vcf_file_md5 in zip(vcf_files, vcf_file_md5):
            if add_browsable:
                file_obj = self.loader.insert_file(
                    project_accession=project_accession, assembly_set_id=1, ena_submission_file_id=1,
                    filename=vcf_file,file_md5=vcf_file_md5, file_type='vcf', file_size=10,  ftp_file='path/to/ftp'
                )
            else:
                # Only create the file object
                file_obj = File(
                    ena_submission_file_id='1',  filename=vcf_file, file_md5=vcf_file_md5,
                    file_type='vcf',
                    file_location=None, file_class='submitted', file_version=1,
                    is_current=1,
                    ftp_file='path/to/ftp'
                )
                self.loader.eva_session.add(file_obj)
            analysis_obj.files.append(file_obj)
            file_objs.append(file_obj)
        for biosample in biosamples:
            sample_obj = self.loader.insert_sample(biosample_accession=biosample, ena_accession='ena_accession')
            sample_objs.append(sample_obj)
        self.loader.eva_session.commit()

        self.loader.begin_or_continue_transaction()
        for file_obj, sample_obj in zip(file_objs,sample_objs):
            self.loader.insert_sample_in_file(file_id=file_obj.file_id, sample_id=sample_obj.sample_id,
                                              name_in_file='sample_name')
        self.loader.eva_session.commit()
        return project_accession, analysis_accession, vcf_files, vcf_file_md5, biosamples

    def test_update_project_samples_temp1(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)

            project_accession, _, _, _, _ = self._load_project_analysis_files_samples()
            self.loader.update_project_samples_temp1(project_accession=project_accession)

            query = select(ProjectSampleTemp1)

            results = []
            for result in self.loader.eva_session.execute(query).fetchall():
                res = result.ProjectSampleTemp1
                results.append((res.project_accession, res.sample_count))

            # Only 2 samples because one sample is contained in two files
            assert results == [('prj000001', 2)]

    def _get_browsable_file_names(self):
        query = select(BrowsableFile)
        results = []
        for browsable_file in self.loader.eva_session.execute(query).scalars():
            results.append(browsable_file.filename)
        return results

    def test_insert_browsable_files_for_project(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            project_accession, _, vcf_files, _, _ = self._load_project_analysis_files_samples()
            browsable_file_names = self._get_browsable_file_names()
            assert browsable_file_names == []
            self.loader.insert_browsable_files_for_project(project_accession=project_accession)
            browsable_file_names = self._get_browsable_file_names()
            assert sorted(browsable_file_names) == vcf_files

    def test_not_insert_browsable_files_for_project(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            project_accession, _, vcf_files, _, _ = self._load_project_analysis_files_samples(add_browsable=True)
            browsable_file_names = self._get_browsable_file_names()
            assert browsable_file_names == vcf_files
            self.loader.insert_browsable_files_for_project(project_accession=project_accession)
            browsable_file_names = self._get_browsable_file_names()
            assert sorted(browsable_file_names) == vcf_files

    def test_mark_release_browsable_files_for_project(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            project_accession, _, _, _, _ = self._load_project_analysis_files_samples(add_browsable=True)
            release_date = date(year=2024, month=1, day=1)
            query = select(BrowsableFile)
            for browsable_file in self.loader.eva_session.execute(query).scalars():
                assert browsable_file.loaded == False
                assert browsable_file.eva_release == 'Unreleased'
            self.loader.mark_release_browsable_files_for_project(project_accession, release_date)

            for browsable_file in self.loader.eva_session.execute(query).scalars():
                assert browsable_file.loaded == True
                assert browsable_file.eva_release == '20240101'

    def test_update_files_with_ftp_path_for_project(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            project_accession, _, _, _, _ = self._load_project_analysis_files_samples(add_browsable=True)
            query = select(File)
            for file_obj in self.loader.eva_session.execute(query).scalars():
                assert file_obj.ftp_file == 'path/to/ftp'
            self.loader.update_files_with_ftp_path_for_project(project_accession)
            ftp_file_paths = []
            for file_obj in self.loader.eva_session.execute(query).scalars():
                ftp_file_paths.append(file_obj.ftp_file)
            assert ftp_file_paths == [
                '/ftp.ebi.ac.uk/pub/databases/eva/prj000001/vcf_file1.vcf',
                '/ftp.ebi.ac.uk/pub/databases/eva/prj000001/vcf_file2.vcf'
            ]

    def test_update_loaded_assembly_in_browsable_files_for_project(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            metadata.create_all(engine)
            project_accession, _, _, _, _ = self._load_project_analysis_files_samples(add_browsable=True)
            query = select(BrowsableFile)
            for browsable_file_obj in self.loader.eva_session.execute(query).scalars():
                assert browsable_file_obj.loaded_assembly is None
            self.loader.update_loaded_assembly_in_browsable_files_for_project(project_accession)
            query = select(BrowsableFile)
            for browsable_file_obj in self.loader.eva_session.execute(query).scalars():
                assert browsable_file_obj.loaded_assembly == 'GCA000001'


    def test_load_clustering_record(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        now=datetime.datetime.now()
        with self.patch_evapro_engine(engine), patch('eva_submission.evapro.populate_evapro.now', return_value=now):
            metadata.create_all(engine)
            self.loader.load_clustering_record(taxonomy=1234, assembly='assembly', clustering_source='source')

        query = select(ClusteredVariantUpdate)
        clustering_update_obj = self.loader.eva_session.execute(query).scalar()
        assert clustering_update_obj.taxonomy_id == 1234
        assert clustering_update_obj.assembly_accession == 'assembly'
        assert clustering_update_obj.source == 'source'
        assert clustering_update_obj.ingestion_time == now
