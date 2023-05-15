import glob
import os
import shutil
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from eva_submission.submission_config import load_config
from eva_submission.submission_qc_checks import EloadQC

class TestSubmissionQC(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

    def tearDown(self):
        projects = glob.glob(os.path.join(self.resources_folder, 'projects', 'PRJEB12345'))
        for proj in projects:
            shutil.rmtree(proj)
        ingest_csv = os.path.join(self.eload.eload_dir, 'vcf_files_to_ingest.csv')
        if os.path.exists(ingest_csv):
            os.remove(ingest_csv)
        self.eload.eload_cfg.content = self.original_cfg

    def _patch_metadata_handle(self):
        return patch('eva_submission.submission_qc_checks.get_metadata_connection_handle', autospec=True)

    def test_submission_qc_checks_failed_1(self):
        self.eload = EloadQC(101)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test3.vcf.gz'], ['test1.vcf.gz']], [[['Homo Sapiens']]]]
            self.assertEqual(self.expected_report_of_eload_101(), self.eload.run_qc_checks_for_submission())

    def test_submission_qc_checks_failed_2(self):
        self.eload = EloadQC(102)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            self.assertEqual(self.expected_report_of_eload_102(), self.eload.run_qc_checks_for_submission())

    def test_submission_qc_checks_passed(self):
        self.eload = EloadQC(103)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

        with self._patch_metadata_handle(), \
                patch('eva_submission.submission_qc_checks.get_all_results_for_query') as m_get_browsable_files:
            m_get_browsable_files.side_effect = [[['test1.vcf.gz'], ['test2.vcf.gz']], [[['Homo Sapiens']]]]
            self.assertEqual(self.expected_report_of_eload_103(), self.eload.run_qc_checks_for_submission())


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
            Remapping Ingestion check: FAIL
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
        
                Error: Error fetching files from ftp for study PRJEB11111
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
            Remapping Ingestion check: FAIL
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
        
                Error: Error fetching files from ftp for study PRJEB22222
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
            Remapping Ingestion check: PASS
            Back-propogation check: PASS
        FTP check: FAIL
        Study check: FAIL
        Study metadata check: FAIL
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
                    
                    pass: PASS
                pass (backpropagation check): PASS
                    
                    pass: PASS
                
        ----------------------------------
        
        FTP check:
        
                Error: Error fetching files from ftp for study PRJEB33333
        ----------------------------------
        
        Study check:
        
                pass: FAIL
        ----------------------------------

        Study metadata check:
        
                pass: FAIL
                missing assemblies: ["['Homo Sapiens'](GCA_000003205.6)"]
        ----------------------------------
        """
