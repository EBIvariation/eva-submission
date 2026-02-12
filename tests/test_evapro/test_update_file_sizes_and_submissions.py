import datetime
import os.path
from unittest import TestCase
from unittest.mock import patch, PropertyMock, Mock

import pytest
from sqlalchemy import create_engine, select

from bin.update_file_sizes_and_submissions import update_file_sizes_and_add_missing_submissions_for_project
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import metadata, Project, Analysis, File, Submission
from eva_submission.submission_config import load_config


class TestUpdateFileSizes(TestCase):

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

    def _setup_project_with_files(self, project_accession='PRJEB00001', analysis_accession='ERZ000001',
                                  files_data=None, submission_data=None):
        """
        Populate the in-memory DB with a Project, Analysis, and File and Submission records linked via junction tables.
        files_data is a list of dicts with keys: filename, file_md5, file_size, file_type (optional).
        submission_data is a list of dicts with keys: accession.
        Returns the list of File and Submission objects created.
        """
        if files_data is None:
            files_data = []
        if submission_data is None:
            submission_data = []

        self.loader.begin_or_continue_transaction()
        project_obj = self.loader.insert_project_in_evapro(
            project_accession=project_accession, center_name='center', project_alias='alias',
            title='title', description='desc', ena_study_type='Other',
            ena_secondary_study_id='ERP000001'
        )
        analysis_obj = self.loader.insert_analysis(
            analysis_accession=analysis_accession, title='title', alias='alias',
            description='desc', center_name='center',
            date=datetime.datetime(2024, 1, 1), assembly_set_id=1
        )
        project_obj.analyses.append(analysis_obj)

        file_objs = []
        for fd in files_data:
            file_obj = File(
                ena_submission_file_id='1',
                filename=fd['filename'],
                file_md5=fd['file_md5'],
                file_type=fd.get('file_type', 'VCF'),
                file_size=fd.get('file_size'),
                file_class='submitted',
                file_version=1,
                is_current=1,
                ftp_file='path/to/ftp'
            )
            self.loader.eva_session.add(file_obj)
            analysis_obj.files.append(file_obj)
            file_objs.append(file_obj)
        submission_objs = []
        for sub in submission_data:
            submission_obj = Submission(
                submission_accession=sub['accession'],
                type = 'submitted',
                action = 'ADD',
                title = 'alias1',
                date = datetime.datetime(2024, 1, 1),
                brokered = 1
            )
            analysis_obj.submissions.append(submission_obj)
            submission_objs.append(submission_obj)

        self.loader.eva_session.commit()
        return file_objs, submission_objs

    def test_update_file_size_happy_path(self):
        """File exists in EVAPRO, MD5 matches, size differs -> updated."""
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            self._setup_project_with_files(
                files_data=[
                    {'filename': 'data.vcf.gz', 'file_md5': 'abc123', 'file_size': None},
                    {'filename': 'data.vcf.gz.tbi', 'file_md5': 'def456', 'file_size': 100},
                ]
            )

            # ENA returns updated sizes for both files
            self.loader.ena_project_finder = PropertyMock(
                find_files_in_ena=Mock(return_value=[
                    ('ERZ000001', 'ERF001', 'data.vcf.gz', 'abc123', 'VCF', 5000, 4),
                    ('ERZ000001', 'ERF002', 'data.vcf.gz.tbi', 'def456', 'TABIX', 200, 4),
                ]),
                find_ena_submission_for_analysis= Mock(return_value=[
                    ('ERA00000001', 'alias1', datetime.datetime(2025, 1, 1),
                    '2025-04-01', {'type': 'ADD', 'schema': 'project', 'source': 'alias1.Project.xml'})
                ])
            )

            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, 'PRJEB00001')

            assert file_updated == 2
            assert file_skipped == 0
            assert file_not_found == 0
            assert file_mismatch == 0
            assert submission_linked == 1
            assert submission_skipped == 0

            # Verify the DB was updated
            files = {f.File.filename: f.File for f in self.loader.eva_session.execute(select(File)).fetchall()}
            assert files['data.vcf.gz'].file_size == 5000
            assert files['data.vcf.gz.tbi'].file_size == 200

            submissions = {s.Submission.submission_accession: s.Submission for s in self.loader.eva_session.execute(select(Submission)).fetchall()}
            assert submissions['ERA00000001'].analyses[0].analysis_accession == 'ERZ000001'

    def test_size_already_correct(self):
        """
        File exists, MD5 matches, size already correct -> skipped.
        Submission exists and is already linked
        """
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            self._setup_project_with_files(
                files_data=[
                    {'filename': 'data.vcf.gz', 'file_md5': 'abc123', 'file_size': 5000},
                ],
                submission_data = [
                    {'accession': 'ERA00000001'}
                ]
            )

            self.loader.ena_project_finder = PropertyMock(
                find_files_in_ena=Mock(return_value=[
                    ('ERZ000001', 'ERF001', 'data.vcf.gz', 'abc123', 'VCF', 5000, 4),
                ]),
                find_ena_submission_for_analysis= Mock(return_value=[
                    ('ERA00000001', 'alias1', datetime.datetime(2025, 1, 1),
                    '2025-04-01', {'type': 'ADD', 'schema': 'project', 'source': 'alias1.Project.xml'})
                ])
            )

            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, 'PRJEB00001')

            assert file_updated == 0
            assert file_skipped == 1
            assert file_not_found == 0
            assert file_mismatch == 0
            assert submission_linked == 0
            assert submission_skipped == 1

    def test_md5_mismatch(self):
        """File found by name but MD5 differs -> counted as not_found. No Submission"""
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            self._setup_project_with_files(
                files_data=[
                    {'filename': 'data.vcf.gz', 'file_md5': 'abc123', 'file_size': 100},
                ]
            )

            # ENA returns a different MD5 for the same filename
            self.loader.ena_project_finder = PropertyMock(
                find_files_in_ena=Mock(return_value=[
                    ('ERZ000001', 'ERF001', 'data.vcf.gz', 'DIFFERENT_MD5', 'VCF', 5000, 4),
                ]),
                find_ena_submission_for_analysis=Mock(return_value=[])
            )

            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, 'PRJEB00001')

            assert file_updated == 0
            assert file_skipped == 0
            assert file_not_found == 0
            assert file_mismatch == 1
            assert submission_linked == 0
            assert submission_skipped == 0
            # Verify the size was NOT changed
            file_obj = self.loader.eva_session.execute(select(File)).fetchone().File
            assert file_obj.file_size == 100

    def test_file_not_in_evapro(self):
        """ENA returns a file not present in EVAPRO -> counted as not_found. no Submission"""
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            self._setup_project_with_files(
                files_data=[
                    {'filename': 'data.vcf.gz', 'file_md5': 'abc123', 'file_size': 100},
                ]
            )

            # ENA returns an extra file that doesn't exist in EVAPRO
            self.loader.ena_project_finder = PropertyMock(
                find_files_in_ena=Mock(return_value=[
                    ('ERZ000001', 'ERF001', 'data.vcf.gz', 'abc123', 'VCF', 5000, 4),
                    ('ERZ000001', 'ERF003', 'missing_file.vcf.gz', 'xyz789', 'VCF', 3000, 4),
                ]),
                find_ena_submission_for_analysis=Mock(return_value=[])
            )

            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, 'PRJEB00001')

            assert file_updated == 1
            assert file_skipped == 0
            assert file_not_found == 1
            assert file_mismatch == 0
            assert submission_linked == 0
            assert submission_skipped == 0

    def test_project_not_found(self):
        """Non-existent project returns all zeros."""
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, 'NONEXISTENT')

            assert file_updated == 0
            assert file_skipped == 0
            assert file_not_found == 0
            assert file_mismatch == 0
            assert submission_linked == 0
            assert submission_skipped == 0


@pytest.mark.skip(reason='Needs access to ERA database')
class TestUpdateFileSizesFromENA(TestCase):
    """
    Uses project PRJEB25731 / analysis ERZ498176 with known file data.
    """
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

    def test_update_file_sizes_from_ena(self):
        """
        Pre-populate EVAPRO with project PRJEB25731 / analysis ERZ498176 and files
        with correct MD5s but null file_size, then verify real ENA data updates them.
        """
        project_accession = 'PRJEB25731'
        analysis_accession = 'ERZ498176'
        # Known files for this analysis (from test_find_from_ena.py test_find_files_in_ena
        # which tests ERZ293539 but analysis for PRJEB25731 is ERZ498176)
        # We retrieve the actual files from ENA first to set up the test data
        files_from_ena = list(self.loader.ena_project_finder.find_files_in_ena(analysis_accession))
        assert len(files_from_ena) > 0, f'No files found in ENA for analysis {analysis_accession}'

        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.patch_evapro_engine(engine):
            # Set up the project and analysis in the in-memory DB
            self.loader.begin_or_continue_transaction()
            project_obj = self.loader.insert_project_in_evapro(
                project_accession=project_accession, center_name='center', project_alias='alias',
                title='title', description='desc', ena_study_type='Other',
                ena_secondary_study_id='ERP000001'
            )
            analysis_obj = self.loader.insert_analysis(
                analysis_accession=analysis_accession, title='title', alias='alias',
                description='desc', center_name='center',
                date=datetime.datetime(2024, 1, 1), assembly_set_id=1
            )
            project_obj.analyses.append(analysis_obj)

            # Insert files with correct MD5s but null file_size
            for ana_acc, sub_file_id, filename, file_md5, file_type, file_size, status_id in files_from_ena:
                file_obj = File(
                    ena_submission_file_id=sub_file_id,
                    filename=filename,
                    file_md5=file_md5,
                    file_type=file_type,
                    file_size=None,
                    file_class='submitted',
                    file_version=1,
                    is_current=1,
                    ftp_file='path/to/ftp'
                )
                self.loader.eva_session.add(file_obj)
                analysis_obj.files.append(file_obj)
            self.loader.eva_session.commit()

            # Run the update using real ENA finder
            (
                file_updated, file_skipped, file_not_found,
                file_mismatch, submission_linked, submission_skipped
            ) = update_file_sizes_and_add_missing_submissions_for_project(self.loader, project_accession)

            assert file_updated == len(files_from_ena)
            assert file_skipped == 0
            assert file_not_found == 0
            assert file_mismatch == 0
            assert submission_linked == 1
            assert submission_skipped == 0

            # Verify all files now have non-null sizes matching ENA
            expected_sizes = {filename: file_size
                              for _, _, filename, _, _, file_size, _ in files_from_ena}
            for result in self.loader.eva_session.execute(select(File)).fetchall():
                file_obj = result.File
                assert file_obj.file_size is not None
                assert file_obj.file_size == expected_sizes[file_obj.filename]
            # Verify that the submission have also been added
            submissions = {s.Submission.submission_accession: s.Submission for s in
                           self.loader.eva_session.execute(select(Submission)).fetchall()}
            assert submissions['ERA1258499'].analyses[0].analysis_accession == 'ERZ498176'
