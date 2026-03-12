import csv
import gzip
import os
import shutil
import subprocess
import tempfile
from unittest import TestCase
from unittest.mock import patch, MagicMock

from eva_submission.study_deprecation import StudyDeprecation
from eva_submission.submission_config import load_config


class TestStudyDeprecation(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        os.chdir(self.top_dir)
        self.output_dir = tempfile.mkdtemp()
        with patch('eva_submission.study_deprecation.EvaProjectLoader'):
            self.deprecation = StudyDeprecation('PRJEB12345', self.output_dir)

    def tearDown(self):
        shutil.rmtree(self.output_dir, ignore_errors=True)

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
        params = mock_nf.call_args[0][1]
        self.assertEqual(params['project_accession'], 'PRJEB12345')
        self.assertEqual(params['drop_study_props'], '/path/drop.properties')
        self.assertEqual(params['deprecation_props'], '/path/deprecation.properties')
        tasks_passed = mock_nf.call_args[0][3]
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
        with patch('eva_submission.study_deprecation.command_utils.run_command_with_output') as mock_cmd, \
                patch('eva_submission.study_deprecation.shutil.rmtree') as mock_rm:
            self.deprecation.run_nextflow(
                'deprecate_study',
                {'valid_deprecations': '/path/valid.csv', 'project_accession': 'PRJEB12345'},
                resume=False,
                tasks=['deprecate_variants', 'drop_study']
            )

        mock_cmd.assert_called_once()
        mock_rm.assert_called_once()

    def test_run_nextflow_failure_preserves_work_dir(self):
        with patch('eva_submission.study_deprecation.command_utils.run_command_with_output',
                   side_effect=subprocess.CalledProcessError(1, 'nextflow')), \
                patch('eva_submission.study_deprecation.shutil.rmtree') as mock_rm:
            with self.assertRaises(subprocess.CalledProcessError):
                self.deprecation.run_nextflow(
                    'deprecate_study',
                    {'valid_deprecations': '/path/valid.csv'},
                    resume=False,
                    tasks=['deprecate_variants']
                )
        mock_rm.assert_not_called()

    def test_run_nextflow_resume_skips_completed_tasks(self):
        # Mark deprecate_variants as already complete
        self.deprecation._set_cfg(
            self.deprecation.config_section, 'deprecate_study', 'nextflow_dir', 'deprecate_variants',
            value=self.deprecation.nextflow_complete_value
        )
        with patch('eva_submission.study_deprecation.command_utils.run_command_with_output') as mock_cmd, \
                patch('eva_submission.study_deprecation.shutil.rmtree'):
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
        with patch('eva_submission.study_deprecation.glob.glob', return_value=[report_path]):
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
        with patch('eva_submission.study_deprecation.glob.glob', return_value=[report_path]):
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

        with patch('eva_submission.study_deprecation.glob.glob', side_effect=fake_glob):
            result = self.deprecation.get_accession_reports_for_project()

        self.assertIn('GCA_000001405.2', result)
        self.assertCountEqual(result['GCA_000001405.2'], [report_a, report_b])

    # -------------------------
    # extract_ss_ids_from_accession_reports
    # -------------------------

    def _write_vcf_gz(self, path, data_lines):
        """Helper: write a minimal gzipped VCF with given data lines (list of tab-separated strings)."""
        with gzip.open(path, 'wt') as f:
            f.write('##fileformat=VCFv4.1\n')
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