import os
from unittest import TestCase, mock
from unittest.mock import patch

from requests import HTTPError

from eva_submission.submission_config import load_config
from eva_submission.submission_qc_checks import EloadQC


class TestSubmissionQC(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        os.chdir(self.top_dir)

    def _patch_metadata_handle(self):
        return patch('eva_submission.submission_qc_checks.get_metadata_connection_handle', autospec=True)

    def _mock_response(self, status=200, content="CONTENT", json_data=None, raise_for_status=None):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status = mock.Mock()
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
        mock_resp.status_code = status
        mock_resp.content = content
        if json_data:
            mock_resp.json = mock.Mock(
                return_value=json_data
            )
        return mock_resp

    def test_submission_qc_checks_failed_1(self):
        self.eload = EloadQC(101)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login'), \
                patch('eva_submission.submission_qc_checks.FTP.cwd'), \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test3.vcf.gz'], ['test1.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down"))]
            m_ftp_nlst.return_value = []
            self.assertEqual(self.expected_report_of_eload_101(), self.eload.run_qc_checks_for_submission())
            self.assertIn(EloadQC.config_section, self.eload.eload_cfg)

    def test_submission_qc_checks_failed_2(self):
        self.eload = EloadQC(102)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login'), \
                patch('eva_submission.submission_qc_checks.FTP.cwd'), \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(
                json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB99999"}]}]}),
                                 self._mock_response(json_data={"response": [
                                     {"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB99999"}]}]})]
            m_ftp_nlst.return_value = ['test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz.csi',
                                       'test1.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_102(), self.eload.run_qc_checks_for_submission())
            self.assertIn(EloadQC.config_section, self.eload.eload_cfg)

    def test_submission_qc_checks_passed(self):
        self.eload = EloadQC(103)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login'), \
                patch('eva_submission.submission_qc_checks.FTP.cwd'), \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_all_results_for_query:
            m_get_all_results_for_query.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [['ecaballus_30']], [['ecaballus_30']]]
            json_with_id = {
                "response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB33333"}]}]
            }
            json_with_project_id = {
                "response": [{"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB33333"}]}]
            }
            m_get.side_effect = [
                self._mock_response(json_data=json_with_id),
                self._mock_response(json_data=json_with_project_id),
                self._mock_response(json_data=json_with_project_id)
            ]
            m_ftp_nlst.return_value = ['test1.vcf.gz', 'test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz',
                                       'test1.accessioned.vcf.gz.csi', 'test1.accessioned.vcf.csi', 'test2.vcf.gz',
                                       'test2.vcf.gz.csi', 'test2.vcf.csi', 'test2.accessioned.vcf.gz',
                                       'test2.accessioned.vcf.gz.csi', 'test2.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_103(), self.eload.run_qc_checks_for_submission())
            self.assertIn(EloadQC.config_section, self.eload.eload_cfg)

    def test_submission_qc_checks_missing_files(self):
        self.eload = EloadQC(104)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login'), \
                patch('eva_submission.submission_qc_checks.FTP.cwd'), \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [
                self._mock_response(json_data={
                    "response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB44444"}]}]
                }),
                self._mock_response(json_data={
                    "response": [{"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB44444"}]}]
                })
            ]
            m_ftp_nlst.return_value = ['test1.vcf.gz', 'test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz',
                                       'test1.accessioned.vcf.gz.csi', 'test1.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_104(), self.eload.run_qc_checks_for_submission())
            self.assertIn(EloadQC.config_section, self.eload.eload_cfg)

    def test_check_if_variant_load_completed_successfully(self):
        self.eload = EloadQC(103)
        result, report = self.eload.check_if_variant_load_completed_successfully()
        assert result == 'PASS'
        assert report == 'Success: PASS'

    def expected_report_of_eload_101(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: FAIL
        Accessioning job check: FAIL
        Variants Skipped accessioning check: PASS with Warning (Manual Check Required)
        Variant load and Accession Import check:
            Variant load check: FAIL
            Annotation check: FAIL
            Variant Statistics check: FAIL
            Study Statistics check: FAIL
            Accession Import check: FAIL
        Remapping and Clustering Check:
            Remapping check: FAIL
            Clustering check: FAIL
            Back-propogation check: FAIL
        FTP check: FAIL
        Study check: FAIL
        Study metadata check: FAIL
        
        QC Details:
        ----------------------------------
        Browsable files check:
            Success : FAIL
            Expected files: ['test1.vcf.gz', 'test2.vcf.gz']
            Missing files: {'test2.vcf.gz'}
        ---------------------------------
        Accessioning job check:
            Success: FAIL
            failed_files:
                test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
                test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        Variants skipped check:
            Success: PASS with Warning (Manual Check Required)
            Failures:
                test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
                test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        Variant load check:
            Success: FAIL
            Errors:
                test1.vcf.gz - load_vcf error : No load_vcf log file found for test1.vcf.gz
                test2.vcf.gz - load_vcf error : No load_vcf log file found for test2.vcf.gz
        ----------------------------------
        Annotation check: 
            Success: FAIL
            Errors:
                ERZ2499196 - annotate_variants error : No annotate_variants log file found for ERZ2499196
        ----------------------------------
        Variant Statistics check: 
            Success: FAIL
            Errors:
                ERZ2499196 - variant-stats error : No variant-stats log file found for ERZ2499196
        ----------------------------------
        Study Statistics check: 
            Success: FAIL
            Errors:
                ERZ2499196 - file-stats error : No file-stats log file found for ERZ2499196
        ----------------------------------
        Accession Import check: 
            Success: FAIL
            Errors:
                test1.vcf.gz - acc_import error : No acc_import log file found for test1.vcf.gz
                test2.vcf.gz - acc_import error : No acc_import log file found for test2.vcf.gz
        ----------------------------------
        Remapping Check:
            Success: DID NOT RUN
        ----------------------------------
        Clustering check:
            Success: DID NOT RUN
        ----------------------------------
        Backpropagation check:
            Success: DID NOT RUN
        ----------------------------------
        FTP check:
            Error: No files found in FTP for study PRJEB11111
        ----------------------------------
        Study check:
            Success: FAIL
        ----------------------------------
        Study metadata check:
            Success: FAIL
                missing assemblies: ["['Homo Sapiens'](GCA_000001000.1)"]
        ----------------------------------
        """

    def expected_report_of_eload_102(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: PASS
        Accessioning job check: FAIL
        Variants Skipped accessioning check: PASS with Warning (Manual Check Required)
        Variant load and Accession Import check:
            Variant load check: FAIL
            Annotation check: FAIL
            Variant Statistics check: FAIL
            Study Statistics check: FAIL
            Accession Import check: FAIL
        Remapping and Clustering Check:
            Remapping check: FAIL
            Clustering check: FAIL
            Back-propogation check: FAIL
        FTP check: FAIL
        Study check: FAIL
        Study metadata check: FAIL
        
        QC Details:
        ----------------------------------
        Browsable files check:
            Success : PASS
            Expected files: ['test1.vcf.gz', 'test2.vcf.gz']
            Missing files: None
        ---------------------------------
        Accessioning job check:
            Success: FAIL
            failed_files:
                test1.vcf.gz - failed job/step : CREATE_SUBSNP_ACCESSION_STEP
                test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        Variants skipped check:
            Success: PASS with Warning (Manual Check Required)
            Failures:
                test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        Variant load check:
            Success: FAIL
            Errors:
                test1.vcf.gz - load_vcf error : No load_vcf log file found for test1.vcf.gz
                test2.vcf.gz - load_vcf error : No load_vcf log file found for test2.vcf.gz
        ----------------------------------
        Annotation check: 
            Success: FAIL
            Errors:
                ERZ2499196 - annotate_variants error : No annotate_variants log file found for ERZ2499196
        ----------------------------------
        Variant Statistics check: 
            Success: FAIL
            Errors:
                ERZ2499196 - variant-stats error : No variant-stats log file found for ERZ2499196
        ----------------------------------
        Study Statistics check: 
            Success: FAIL
            Errors:
                ERZ2499196 - file-stats error : No file-stats log file found for ERZ2499196
        ----------------------------------
        Accession Import check: 
            Success: FAIL
            Errors:
                test1.vcf.gz - acc_import failed job/step : accession-import-step
                test2.vcf.gz - acc_import error : No acc_import log file found for test2.vcf.gz
        ----------------------------------
        Remapping Check:
            Success: DID NOT RUN
        ----------------------------------
        Clustering check:
            Success: DID NOT RUN
        ----------------------------------
        Backpropagation check:
            Success: DID NOT RUN
        ----------------------------------
        FTP check:
            Success: FAIL 
                Missing files: ['test1.vcf.gz', 'test1.accessioned.vcf.gz', 'test2.vcf.gz', 'test2.vcf.gz.csi or test2.vcf.csi', 'test2.accessioned.vcf.gz', 'test2.accessioned.vcf.gz.csi or test2.accessioned.vcf.csi']
        ----------------------------------
        Study check:
            Success: FAIL
        ----------------------------------
        Study metadata check:
            Success: FAIL
                missing assemblies: ["['Homo Sapiens'](GCA_000001000.1)"]
        ----------------------------------
        """

    def expected_report_of_eload_103(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: PASS
        Accessioning job check: PASS
        Variants Skipped accessioning check: PASS
        Variant load and Accession Import check:
            Variant load check: PASS
            Annotation check: SKIP
            Variant Statistics check: PASS
            Study Statistics check: PASS
            Accession Import check: PASS
        Remapping and Clustering Check:
            Remapping check: PASS
            Clustering check: PASS
            Back-propogation check: PASS
        FTP check: PASS
        Study check: PASS
        Study metadata check: PASS
        
        QC Details:
        ----------------------------------
        Browsable files check:
            Success : PASS
            Expected files: ['test1.vcf.gz', 'test2.vcf.gz']
            Missing files: None
        ---------------------------------
        Accessioning job check:
            Success: PASS
        ----------------------------------
        Variants skipped check:
            Success: PASS
        ----------------------------------
        Variant load check:
            Success: PASS
        ----------------------------------
        Annotation check: 
            Annotation result - SKIPPED (no VEP cache)
        ----------------------------------
        Variant Statistics check: 
            Success: PASS
        ----------------------------------
        Study Statistics check: 
            Success: PASS
        ----------------------------------
        Accession Import check: 
            Success: PASS
        ----------------------------------
        Remapping Check:
            Source assembly GCA_000003205.6:
                - vcf_extractor_result : PASS - No Error
                - remapping_ingestion_result: PASS - No Error
            Source assembly GCA_000003205.1:
                - vcf_extractor_result : PASS - No Error
                - remapping_ingestion_result: PASS - No Error
        ----------------------------------
        Clustering check:
            Clustering Job: PASS - No error
            Clustering QC Job: PASS - No error
        ----------------------------------
        Backpropagation check:
            Backpropagation result to GCA_000003205.6: PASS - No Error
            Backpropagation result to GCA_000003205.1: PASS - No Error
        ----------------------------------
        FTP check:
            Success: PASS 
                Missing files: None
        ----------------------------------
        Study check:
            Success: PASS
        ----------------------------------
        Study metadata check:
            Success: PASS
                missing assemblies: None
        ----------------------------------
        """

    def expected_report_of_eload_104(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: PASS
        Accessioning job check: FAIL
        Variants Skipped accessioning check: PASS with Warning (Manual Check Required)
        Variant load and Accession Import check:
            Variant load check: FAIL
            Annotation check: FAIL
            Variant Statistics check: SKIP
            Study Statistics check: SKIP
            Accession Import check: FAIL
        Remapping and Clustering Check:
            Remapping check: FAIL
            Clustering check: FAIL
            Back-propogation check: FAIL
        FTP check: PASS
        Study check: PASS
        Study metadata check: PASS
        
        QC Details:
        ----------------------------------
        Browsable files check:
            Success : PASS
            Expected files: ['test1.vcf.gz']
            Missing files: None
        ---------------------------------
        Accessioning job check:
            Success: FAIL
            failed_files:
                test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
        ----------------------------------
        Variants skipped check:
            Success: PASS with Warning (Manual Check Required)
            Failures:
                test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
        ----------------------------------
        Variant load check:
            Success: FAIL
            Errors:
                test1.vcf.gz - load_vcf error : No load_vcf log file found for test1.vcf.gz
        ----------------------------------
        Annotation check: 
            Success: FAIL
            Errors:
                ERZ2499196 - annotate_variants error : No annotate_variants log file found for ERZ2499196
        ----------------------------------
        Variant Statistics check: 
            Variant statistics result - SKIPPED (aggregated VCF)
        ----------------------------------
        Study Statistics check: 
            Study statistics result - SKIPPED (aggregated VCF)
        ----------------------------------
        Accession Import check: 
            Success: FAIL
            Errors:
                test1.vcf.gz - acc_import error : No acc_import log file found for test1.vcf.gz
        ----------------------------------
        Remapping Check:
            Source assembly GCA_000003205.6:
                - vcf_extractor_result : FAIL - vcf_extractor error : No vcf_extractor log file found for GCA_000003205.6
                - remapping_ingestion_result: FAIL - remapping_ingestion error : No remapping_ingestion log file found for GCA_000003205.6
        ----------------------------------
        Clustering check:
            Clustering Job: FAIL - clustering error : No clustering log file found for GCA_000247795.2
            Clustering QC Job: FAIL - clustering_qc error : No clustering_qc log file found for GCA_000247795.2
        ----------------------------------
        Backpropagation check:
            Backpropagation result to GCA_000003205.6: FAIL - backpropagation error : No backpropagation log file found for GCA_000003205.6
        ----------------------------------
        FTP check:
            Success: PASS 
                Missing files: None
        ----------------------------------
        Study check:
            Success: PASS
        ----------------------------------
        Study metadata check:
            Success: PASS
                missing assemblies: None
        ----------------------------------
        """