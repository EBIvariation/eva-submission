import csv
import gzip
import os
import shutil
import subprocess
import tempfile
from copy import deepcopy
from unittest import TestCase, mock
from unittest.mock import patch, MagicMock, PropertyMock, call

import yaml
from sqlalchemy import create_engine

from eva_submission.eload_deprecation import StudyDeprecation
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import metadata, Project
from eva_submission.submission_config import load_config


class TestStudyDeprecation(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        os.chdir(self.top_dir)
        self.output_dir = tempfile.mkdtemp()
        with patch('eva_submission.eload_deprecation.EvaProjectLoader'):
            self.deprecation = StudyDeprecation('PRJEB12345', self.output_dir)

    def tearDown(self):
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def _patch_metadata_engine(self):
        engine = create_engine('sqlite+pysqlite:///:memory:', future=True)
        metadata.create_all(engine)
        return patch.object(EvaProjectLoader, '_evapro_engine', side_effect=PropertyMock(return_value=engine))

    # -------------------------
    # CSV generation
    # -------------------------

    def test_create_deprecation_csv(self):
        assembly_db_pairs = [('GCA_000001405.2', 'eva_hsapiens_grch37')]
        variant_id_files = {'GCA_000001405.2': '/path/to/ssids.txt'}

        csv_path = self.deprecation.create_deprecation_csv(assembly_db_pairs, variant_id_files)

        self.assertTrue(os.path.exists(csv_path))
        with open(csv_path, newline='') as f:
            rows = list(csv.DictReader(f))

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['assembly_accession'], 'GCA_000001405.2')
        self.assertEqual(row['variant_id_file'], '/path/to/ssids.txt')
        self.assertEqual(row['db_name'], 'eva_hsapiens_grch37')
        self.assertNotIn('deprecation_properties_file', row)

    def test_create_deprecation_csv_missing_file_raises(self):
        assembly_db_pairs = [('GCA_000001405.2', 'eva_hsapiens_grch37')]
        variant_id_files = {}  # missing entry for the assembly
        with self.assertRaises(ValueError):
            self.deprecation.create_deprecation_csv(assembly_db_pairs, variant_id_files)

    def test_create_deprecation_csv_multiple_assemblies(self):
        assembly_db_pairs = [
            ('GCA_000001405.2', 'eva_hsapiens_grch37'),
            ('GCA_000001405.3', 'eva_hsapiens_grch38'),
        ]
        variant_id_files = {
            'GCA_000001405.2': '/path/to/ssids_37.txt',
            'GCA_000001405.3': '/path/to/ssids_38.txt',
        }
        csv_path = self.deprecation.create_deprecation_csv(assembly_db_pairs, variant_id_files)

        with open(csv_path, newline='') as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 2)
        assemblies = [r['assembly_accession'] for r in rows]
        self.assertIn('GCA_000001405.2', assemblies)
        self.assertIn('GCA_000001405.3', assemblies)

    # -------------------------
    # Properties file generation
    # -------------------------

    def test_create_deprecation_properties(self):
        with patch.object(self.deprecation.properties_generator,
                          '_common_accessioning_clustering_properties',
                          return_value={'spring.batch.job.names': None}) as mock_common, \
                patch.object(self.deprecation.properties_generator,
                             '_format', return_value='mocked_properties') as mock_format:
            props_path = self.deprecation.create_deprecation_properties('PRJEB12345_OBSOLETE', 'Withdrawn')

        self.assertEqual(props_path, os.path.join(self.output_dir, 'variant_deprecation.properties'))
        self.assertTrue(os.path.exists(props_path))
        with open(props_path) as f:
            self.assertEqual(f.read(), 'mocked_properties')

        mock_common.assert_called_once_with(
            assembly_accession=None,
            read_preference='secondaryPreferred',
            chunk_size=100
        )
        format_call_args = mock_format.call_args
        extra_props = format_call_args[0][1]
        self.assertEqual(extra_props['spring.batch.job.names'],
                         'DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_JOB')
        self.assertEqual(extra_props['parameters.deprecationIdSuffix'], 'PRJEB12345_OBSOLETE')
        self.assertEqual(extra_props['parameters.deprecationReason'], 'Withdrawn')
        self.assertNotIn('parameters.variantIdFile', extra_props)

    def test_create_drop_study_properties(self):
        with patch.object(self.deprecation.properties_generator,
                          'get_accession_import_properties',
                          return_value='mocked_drop_props') as mock_get_props:
            props_path = self.deprecation.create_drop_study_properties()

        self.assertEqual(props_path, os.path.join(self.output_dir, 'drop_study.properties'))
        self.assertTrue(os.path.exists(props_path))
        with open(props_path) as f:
            self.assertEqual(f.read(), 'mocked_drop_props')
        mock_get_props.assert_called_once()

    # -------------------------
    # EVAPRO updates
    # -------------------------

    def test_mark_project_inactive_in_evapro(self):
        with patch.object(self.deprecation.loader, 'mark_project_inactive') as mock_inactive, \
                patch.object(self.deprecation.loader, 'mark_analyses_hidden') as mock_hidden, \
                patch.object(self.deprecation.loader, 'refresh_study_browser') as mock_refresh:
            self.deprecation.mark_project_inactive_in_evapro()

        mock_inactive.assert_called_once_with('PRJEB12345')
        mock_hidden.assert_called_once_with('PRJEB12345')
        mock_refresh.assert_called_once()

    def test_mark_project_inactive_in_evapro_loader(self):
        """Test EvaProjectLoader.mark_project_inactive and mark_analyses_hidden via in-memory DB."""
        with self._patch_metadata_engine():
            loader = EvaProjectLoader()
            # mark_project_inactive on a non-existent project should not raise (update affects 0 rows)
            loader.mark_project_inactive('PRJEB99999')

            # mark_analyses_hidden: inject a mock project with no analyses; should run without error
            mock_project = MagicMock()
            mock_project.analyses = []
            loader.eva_session.get = MagicMock(return_value=mock_project)
            loader.mark_analyses_hidden('PRJEB99999')
            # If analyses is empty, get() should have been called once
            loader.eva_session.get.assert_called_once_with(Project, 'PRJEB99999')

    # -------------------------
    # Nextflow invocation
    # -------------------------

    def test_run_deprecate_study_workflow_both_tasks(self):
        with patch.object(self.deprecation, 'create_drop_study_properties',
                          return_value='/path/drop.properties'), \
                patch.object(self.deprecation, 'create_deprecation_properties',
                             return_value='/path/deprecation.properties'), \
                patch.object(self.deprecation, 'run_nextflow') as mock_nf:
            self.deprecation.run_deprecate_study_workflow(
                resume=False, tasks=['deprecate_variants', 'drop_study'], source_csv_path='path/source.csv',
                deprecation_suffix='PRJEB12345_OBSOLETE', deprecation_reason='Withdrawn'
            )

        mock_nf.assert_called_once()
        call_args = mock_nf.call_args
        params = call_args[0][1]
        self.assertIn('valid_deprecations', params)
        self.assertIn('project_accession', params)
        self.assertEqual(params['project_accession'], 'PRJEB12345')
        self.assertEqual(params['drop_study_props'], '/path/drop.properties')
        self.assertEqual(params['deprecation_props'], '/path/deprecation.properties')
        self.assertIn('jar', params)
        tasks_passed = call_args[0][3]
        self.assertIn('deprecate_variants', tasks_passed)
        self.assertIn('drop_study', tasks_passed)

    def test_run_deprecate_study_workflow_mark_inactive_only(self):
        """mark_inactive is not a Nextflow task; workflow should not be invoked."""
        with patch.object(self.deprecation, 'run_nextflow') as mock_nf:
            self.deprecation.run_deprecate_study_workflow(
                resume=False, tasks=['mark_inactive'], source_csv_path='path/source.csv',
                deprecation_suffix='SUFFIX', deprecation_reason='reason'
            )
        mock_nf.assert_not_called()

    def test_run_nextflow_success(self):
        with patch('eva_submission.eload_deprecation.command_utils.run_command_with_output') as mock_cmd, \
                patch('eva_submission.eload_deprecation.shutil.rmtree') as mock_rm:
            self.deprecation.run_nextflow(
                'deprecate_study',
                {'valid_deprecations': '/path/valid.csv', 'project_accession': 'PRJEB12345'},
                resume=False,
                tasks=['deprecate_variants', 'drop_study']
            )

        mock_cmd.assert_called_once()
        mock_rm.assert_called_once()
        # Both tasks should be marked complete in the config
        self.assertEqual(
            self.deprecation._get_cfg(self.deprecation.config_section, 'deprecate_study',
                                      'nextflow_dir', 'deprecate_variants'),
            self.deprecation.nextflow_complete_value
        )
        self.assertEqual(
            self.deprecation._get_cfg(self.deprecation.config_section, 'deprecate_study',
                                      'nextflow_dir', 'drop_study'),
            self.deprecation.nextflow_complete_value
        )

    def test_run_nextflow_failure_preserves_work_dir(self):
        with patch('eva_submission.eload_deprecation.command_utils.run_command_with_output',
                   side_effect=subprocess.CalledProcessError(1, 'nextflow')):
            with self.assertRaises(subprocess.CalledProcessError):
                self.deprecation.run_nextflow(
                    'deprecate_study',
                    {'valid_deprecations': '/path/valid.csv'},
                    resume=False,
                    tasks=['deprecate_variants']
                )
        # Work dir should NOT have been removed on failure
        work_dir = self.deprecation._get_cfg(
            self.deprecation.config_section, 'deprecate_study', 'nextflow_dir', 'deprecate_variants'
        )
        self.assertNotEqual(work_dir, self.deprecation.nextflow_complete_value)

    def test_run_nextflow_resume_skips_completed_tasks(self):
        # Mark deprecate_variants as already complete
        self.deprecation._set_cfg(
            self.deprecation.config_section, 'deprecate_study', 'nextflow_dir', 'deprecate_variants',
            value=self.deprecation.nextflow_complete_value
        )
        with patch('eva_submission.eload_deprecation.command_utils.run_command_with_output') as mock_cmd, \
                patch('eva_submission.eload_deprecation.shutil.rmtree'):
            returned_tasks = self.deprecation.run_nextflow(
                'deprecate_study',
                {'valid_deprecations': '/path/valid.csv'},
                resume=True,
                tasks=['deprecate_variants', 'drop_study']
            )

        mock_cmd.assert_called_once()
        # Only drop_study should remain after skipping the completed deprecate_variants
        self.assertEqual(returned_tasks, ['drop_study'])

    # -------------------------
    # Main deprecate() entry point
    # -------------------------

    def test_deprecate_all_tasks(self):
        assembly_accession_reports = {'GCA_000001405.2': ['/path/report.accessioned.vcf.gz']}
        assembly_db_pairs = [('GCA_000001405.2', 'eva_hsapiens_grch37')]
        expected_variant_id_file = os.path.join(self.output_dir, 'GCA_000001405.2_variant_ids.txt')

        with patch.object(self.deprecation, 'get_assemblies_and_db_names',
                          return_value=assembly_db_pairs), \
                patch.object(self.deprecation, 'extract_ss_ids_from_accession_reports') as mock_extract, \
                patch.object(self.deprecation, 'create_deprecation_csv') as mock_csv, \
                patch.object(self.deprecation, 'run_deprecate_study_workflow') as mock_nf, \
                patch.object(self.deprecation, 'mark_project_inactive_in_evapro') as mock_mark:
            self.deprecation.deprecate(
                assembly_accession_reports, 'PRJEB12345_OBSOLETE', 'Withdrawn',
                tasks=['deprecate_variants', 'drop_study', 'mark_inactive']
            )

        mock_extract.assert_called_once_with(
            ['/path/report.accessioned.vcf.gz'], expected_variant_id_file
        )
        mock_csv.assert_called_once_with(
            assembly_db_pairs, {'GCA_000001405.2': expected_variant_id_file}
        )
        mock_nf.assert_called_once()
        mock_mark.assert_called_once()

    def test_deprecate_mark_inactive_only(self):
        """mark_inactive standalone: no Nextflow, no SS extraction, no CSV."""
        with patch.object(self.deprecation, 'get_assemblies_and_db_names') as mock_asm, \
                patch.object(self.deprecation, 'extract_ss_ids_from_accession_reports') as mock_extract, \
                patch.object(self.deprecation, 'create_deprecation_csv') as mock_csv, \
                patch.object(self.deprecation, 'run_deprecate_study_workflow') as mock_nf, \
                patch.object(self.deprecation, 'mark_project_inactive_in_evapro') as mock_mark:
            self.deprecation.deprecate(
                {}, 'SUFFIX', 'reason', tasks=['mark_inactive']
            )

        mock_asm.assert_not_called()
        mock_extract.assert_not_called()
        mock_csv.assert_not_called()
        mock_nf.assert_not_called()
        mock_mark.assert_called_once()

    def test_deprecate_nextflow_only(self):
        """deprecate_variants + drop_study without mark_inactive."""
        assembly_accession_reports = {'GCA_000001405.2': ['/path/report.accessioned.vcf.gz']}
        assembly_db_pairs = [('GCA_000001405.2', 'eva_hsapiens_grch37')]

        with patch.object(self.deprecation, 'get_assemblies_and_db_names',
                          return_value=assembly_db_pairs), \
                patch.object(self.deprecation, 'extract_ss_ids_from_accession_reports'), \
                patch.object(self.deprecation, 'create_deprecation_csv'), \
                patch.object(self.deprecation, 'run_deprecate_study_workflow') as mock_nf, \
                patch.object(self.deprecation, 'mark_project_inactive_in_evapro') as mock_mark:
            self.deprecation.deprecate(
                assembly_accession_reports, 'SUFFIX', 'reason',
                tasks=['deprecate_variants', 'drop_study']
            )

        mock_nf.assert_called_once()
        mock_mark.assert_not_called()

    # -------------------------
    # get_accession_reports_for_project
    # -------------------------

    def test_get_accession_reports_for_project(self):
        eload_result = MagicMock()
        eload_result.fetchall.return_value = [(44,)]
        files_result = MagicMock()
        files_result.fetchall.return_value = [('myfile.vcf.gz', 'GCA_000001405.2')]
        self.deprecation.loader.eva_session.execute = MagicMock(
            side_effect=[eload_result, files_result]
        )

        report_path = 'tests/resources/eloads/ELOAD_44/60_eva_public/myfile.accessioned.vcf.gz'
        with patch('eva_submission.eload_deprecation.glob.glob', return_value=[report_path]):
            result = self.deprecation.get_accession_reports_for_project()

        self.assertEqual(result, {'GCA_000001405.2': [report_path]})

    def test_get_accession_reports_for_project_no_eload_raises(self):
        eload_result = MagicMock()
        eload_result.fetchall.return_value = []
        self.deprecation.loader.eva_session.execute = MagicMock(return_value=eload_result)

        with self.assertRaises(ValueError, msg='No eload ID found'):
            self.deprecation.get_accession_reports_for_project()

    def test_get_accession_reports_for_project_unmatched_report(self):
        """A report with no matching EVAPRO file emits a warning and is excluded."""
        eload_result = MagicMock()
        eload_result.fetchall.return_value = [(44,)]
        files_result = MagicMock()
        files_result.fetchall.return_value = []  # no files in EVAPRO
        self.deprecation.loader.eva_session.execute = MagicMock(
            side_effect=[eload_result, files_result]
        )

        report_path = 'tests/resources/eloads/ELOAD_44/60_eva_public/unknown.accessioned.vcf.gz'
        with patch('eva_submission.eload_deprecation.glob.glob', return_value=[report_path]):
            result = self.deprecation.get_accession_reports_for_project()

        self.assertEqual(result, {})

    def test_get_accession_reports_for_project_multiple_eloads(self):
        """Two eloads for the same project, each contributing a report for the same assembly."""
        eload_result = MagicMock()
        eload_result.fetchall.return_value = [(44,), (55,)]
        files_result = MagicMock()
        files_result.fetchall.return_value = [
            ('file_a.vcf.gz', 'GCA_000001405.2'),
            ('file_b.vcf.gz', 'GCA_000001405.2'),
        ]
        self.deprecation.loader.eva_session.execute = MagicMock(
            side_effect=[eload_result, files_result]
        )

        report_a = 'tests/resources/eloads/ELOAD_44/60_eva_public/file_a.accessioned.vcf.gz'
        report_b = 'tests/resources/eloads/ELOAD_55/60_eva_public/file_b.accessioned.vcf.gz'

        def fake_glob(pattern):
            if 'ELOAD_44' in pattern:
                return [report_a]
            if 'ELOAD_55' in pattern:
                return [report_b]
            return []

        with patch('eva_submission.eload_deprecation.glob.glob', side_effect=fake_glob):
            result = self.deprecation.get_accession_reports_for_project()

        self.assertIn('GCA_000001405.2', result)
        self.assertCountEqual(result['GCA_000001405.2'], [report_a, report_b])

    # -------------------------
    # extract_ss_ids_from_accession_reports
    # -------------------------

    def _write_vcf_gz(self, path, data_lines, extra_headers=None):
        """Helper: write a minimal gzipped VCF with given data lines (list of tab-separated strings)."""
        with gzip.open(path, 'wt') as f:
            f.write('##fileformat=VCFv4.1\n')
            if extra_headers:
                for h in extra_headers:
                    f.write(h + '\n')
            f.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n')
            for line in data_lines:
                f.write(line + '\n')

    def test_extract_ss_ids_from_accession_reports(self):
        report_path = os.path.join(self.output_dir, 'test.accessioned.vcf.gz')
        self._write_vcf_gz(report_path, [
            '1\t100\tss1234567\tA\tT\t.\t.\t.',
            '1\t200\tss9876543\tC\tG\t.\t.\t.',
        ])

        output_path = os.path.join(self.output_dir, 'ss_ids.txt')
        result = self.deprecation.extract_ss_ids_from_accession_reports([report_path], output_path)

        self.assertEqual(result, output_path)
        with open(output_path) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines, ['1234567', '9876543'])

    def test_extract_ss_ids_skips_non_ss_ids(self):
        """Dot IDs and rs IDs in the ID column are ignored."""
        report_path = os.path.join(self.output_dir, 'test.accessioned.vcf.gz')
        self._write_vcf_gz(report_path, [
            '1\t100\tss111\tA\tT\t.\t.\t.',
            '1\t200\t.\tC\tG\t.\t.\t.',       # missing ID
            '1\t300\trs999\tA\tC\t.\t.\t.',   # rs ID
        ])

        output_path = os.path.join(self.output_dir, 'ss_ids.txt')
        self.deprecation.extract_ss_ids_from_accession_reports([report_path], output_path)

        with open(output_path) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines, ['111'])

    def test_extract_ss_ids_multiple_reports(self):
        """IDs from multiple report files are combined into one output file."""
        report_a = os.path.join(self.output_dir, 'a.accessioned.vcf.gz')
        report_b = os.path.join(self.output_dir, 'b.accessioned.vcf.gz')
        self._write_vcf_gz(report_a, ['1\t100\tss111\tA\tT\t.\t.\t.'])
        self._write_vcf_gz(report_b, ['1\t200\tss222\tC\tG\t.\t.\t.'])

        output_path = os.path.join(self.output_dir, 'ss_ids.txt')
        self.deprecation.extract_ss_ids_from_accession_reports([report_a, report_b], output_path)

        with open(output_path) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines, ['111', '222'])
