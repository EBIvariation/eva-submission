import os
from copy import deepcopy
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

    def _mock_response(self, status=200,content="CONTENT", json_data=None, raise_for_status=None):
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
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login') as m_ftp_login, \
                patch('eva_submission.submission_qc_checks.FTP.cwd') as m_ftp_cwd, \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test3.vcf.gz'], ['test1.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),
                                 self._mock_response(status=500, raise_for_status=HTTPError("service is down")),]
            m_ftp_nlst.return_value = []
            self.assertEqual(self.expected_report_of_eload_101(), self.eload.run_qc_checks_for_submission())

    def test_submission_qc_checks_failed_2(self):
        self.eload = EloadQC(102)
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login') as m_ftp_login, \
                patch('eva_submission.submission_qc_checks.FTP.cwd') as m_ftp_cwd, \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(
                json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB99999"}]}]}),
                                 self._mock_response(json_data={"response": [
                                     {"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB99999"}]}]})]
            m_ftp_nlst.return_value = ['test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz.csi', 'test1.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_102(), self.eload.run_qc_checks_for_submission())

    def test_submission_qc_checks_passed(self):
        self.eload = EloadQC(103)
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login') as m_ftp_login, \
                patch('eva_submission.submission_qc_checks.FTP.cwd') as m_ftp_cwd, \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB33333"}]}]}),
                                 self._mock_response(json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB33333"}]}]})]
            m_ftp_nlst.return_value = ['test1.vcf.gz', 'test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz',
                                       'test1.accessioned.vcf.gz.csi', 'test1.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_103(), self.eload.run_qc_checks_for_submission())

    def test_submission_qc_checks_missing_files(self):
        self.eload = EloadQC(104)
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.FTP.login') as m_ftp_login, \
                patch('eva_submission.submission_qc_checks.FTP.cwd') as m_ftp_cwd, \
                patch('eva_submission.submission_qc_checks.FTP.nlst') as m_ftp_nlst, \
                patch('eva_submission.submission_qc_checks.requests.get') as m_get, \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            m_get.side_effect = [self._mock_response(json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"id": "PRJEB44444"}]}]}),
                                 self._mock_response(json_data={"response": [{"numResults": 1, "numTotalResults": 1, "result": [{"studyId": "PRJEB44444"}]}]})]
            m_ftp_nlst.return_value = ['test1.vcf.gz', 'test1.vcf.gz.csi', 'test1.vcf.csi', 'test1.accessioned.vcf.gz',
                                       'test1.accessioned.vcf.gz.csi', 'test1.accessioned.vcf.csi']
            self.assertEqual(self.expected_report_of_eload_104(), self.eload.run_qc_checks_for_submission())


    def expected_report_of_eload_101(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: FAIL
        Accessioning job check: FAIL
        Variants Skipped accessioning check: PASS with Warning (Manual Check Required)
        Variant load check: FAIL
        Remapping and Clustering Check: 
            Clustering check: FAIL 
            Remapping check: FAIL
            Back-propogation check: FAIL
        FTP check: FAIL
        Study check: FAIL
        Study metadata check: FAIL
        ----------------------------------

        Browsable files check:
        
            pass : FAIL
            expected files: ['test1.vcf.gz', 'test2.vcf.gz']
            missing files: {'test2.vcf.gz'}
        ---------------------------------
        
        Accessioning job check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
                    test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        
        Variants skipped check:
        
                pass: PASS with Warning (Manual Check Required)
                failed_files:
                    test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
                    test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        
        Variant load check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - Variant Load Error : No pipeline file found for test1.vcf.gz
                    test2.vcf.gz - Variant Load Error : No pipeline file found for test2.vcf.gz
        ----------------------------------

        Remapping and Clustering check:
        
                pass (clustering check): FAIL
                pass (remapping check): FAIL    
                pass (backpropagation check): FAIL
                Remapping and clustering have not run for this study (or eload configuration file is missing taxonomy)
                Note: This results might not be accurate for older studies. It is advisable to checks those manually
                
        ----------------------------------
        
        FTP check:
        
                Error: No files found in FTP for study PRJEB11111
        ----------------------------------
        
        Study check:
        
                pass: FAIL
        ----------------------------------

        Study metadata check:
        
                pass: FAIL
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
        Variant load check: FAIL
        Remapping and Clustering Check: 
            Clustering check: FAIL 
            Remapping check: FAIL
            Back-propogation check: FAIL
        FTP check: FAIL
        Study check: FAIL
        Study metadata check: FAIL
        ----------------------------------

        Browsable files check:
        
            pass : PASS
            expected files: ['test1.vcf.gz', 'test2.vcf.gz']
            missing files: None
        ---------------------------------
        
        Accessioning job check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - failed_job - CREATE_SUBSNP_ACCESSION_STEP
                    test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        
        Variants skipped check:
        
                pass: PASS with Warning (Manual Check Required)
                failed_files:
                    test2.vcf.gz - Accessioning Error : No accessioning file found for test2.vcf.gz
        ----------------------------------
        
        Variant load check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - failed_job - load-variants-step
                    test2.vcf.gz - Variant Load Error : No pipeline file found for test2.vcf.gz
        ----------------------------------

        Remapping and Clustering check:
        
                pass (clustering check): FAIL
                pass (remapping check): FAIL    
                pass (backpropagation check): FAIL
                Remapping and clustering have not run for this study (or eload configuration file is missing taxonomy)
                Note: This results might not be accurate for older studies. It is advisable to checks those manually
                
        ----------------------------------
        
        FTP check:
        
                pass: FAIL 
                missing files: ['test1.vcf.gz', 'test1.accessioned.vcf.gz', 'test2.vcf.gz', 'test2.vcf.gz.csi or test2.vcf.csi', 'test2.accessioned.vcf.gz', 'test2.accessioned.vcf.gz.csi or test2.accessioned.vcf.csi']
        ----------------------------------
        
        Study check:
        
                pass: FAIL
        ----------------------------------

        Study metadata check:
        
                pass: FAIL
                missing assemblies: ["[\'Homo Sapiens\'](GCA_000001000.1)"]
        ----------------------------------
        """

    def expected_report_of_eload_103(self):
        return """
        QC Result Summary:
        ------------------
        Browsable files check: PASS
        Accessioning job check: PASS
        Variants Skipped accessioning check: PASS
        Variant load check: PASS
        Remapping and Clustering Check: 
            Clustering check: PASS 
            Remapping check: PASS
            Back-propogation check: PASS
        FTP check: PASS
        Study check: PASS
        Study metadata check: PASS
        ----------------------------------

        Browsable files check:
        
            pass : PASS
            expected files: ['test1.vcf.gz']
            missing files: None
        ---------------------------------
        
        Accessioning job check:
        
                pass: PASS
        ----------------------------------
        
        Variants skipped check:
        
                pass: PASS
        ----------------------------------
        
        Variant load check:
        
                pass: PASS
        ----------------------------------

        Remapping and Clustering check:
        
                pass (clustering check): PASS
                    Clustering Job: PASS        
                        
                    Clustering QC Job: PASS
                        
        
                pass (remapping check): PASS
                    
                pass (backpropagation check): PASS
                    
                
        ----------------------------------
        
        FTP check:
        
                pass: PASS 
                missing files: None
        ----------------------------------
        
        Study check:
        
                pass: PASS
        ----------------------------------

        Study metadata check:
        
                pass: PASS
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
        Variant load check: FAIL
        Remapping and Clustering Check: 
            Clustering check: FAIL 
            Remapping check: FAIL
            Back-propogation check: FAIL
        FTP check: PASS
        Study check: PASS
        Study metadata check: PASS
        ----------------------------------

        Browsable files check:
        
            pass : PASS
            expected files: ['test1.vcf.gz']
            missing files: None
        ---------------------------------
        
        Accessioning job check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
        ----------------------------------
        
        Variants skipped check:
        
                pass: PASS with Warning (Manual Check Required)
                failed_files:
                    test1.vcf.gz - Accessioning Error : No accessioning file found for test1.vcf.gz
        ----------------------------------
        
        Variant load check:
        
                pass: FAIL
                failed_files:
                    test1.vcf.gz - Variant Load Error : No pipeline file found for test1.vcf.gz
        ----------------------------------

        Remapping and Clustering check:
        
                pass (clustering check): FAIL
                    Clustering Job: FAIL        
                        Clustering Error : No clustering file found for GCA_000247795.2_clustering.log
                    Clustering QC Job: FAIL
                        Clustering QC Error : No clustering qc file found for GCA_000247795.2_clustering_qc.log
        
                pass (remapping check): FAIL
                    failed_remapping for assemblies:
                        GCA_000003205.6: 
                            - VCF Extractor Error: No vcf extractor file found for GCA_000003205.6_vcf_extractor.log
                            - Remapping Ingestion Error: No remapping ingestion file found for GCA_000003205.6_eva_remapped.vcf_ingestion.log
                        
                pass (backpropagation check): FAIL
                    failed_backpropagation for assemblies:
                        GCA_000003205.6 - Backpropagation Error: No backpropagation file found for GCA_000247795.2_backpropagate_to_GCA_000003205.6.log
                
        ----------------------------------
        
        FTP check:
        
                pass: PASS 
                missing files: None
        ----------------------------------
        
        Study check:
        
                pass: PASS
        ----------------------------------

        Study metadata check:
        
                pass: PASS
                missing assemblies: None
        ----------------------------------
        """