import os
import unittest
import yaml

from eva_submission import ROOT_DIR
#from eva_submission.eload_utils import backup_file
from covid19_submission.ingest_covid19_submission import ingest_covid19_submission



class TestIngestCovid19Submission(unittest.TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'covid19_submission')
    config_file = os.path.join(resources_folder, 'app_config.yml')

    def setUp(self):
        dummy_binary = os.path.join(self.resources_folder, "dummy_binary.sh")
        app_config = {
            "nextflow_binary": "nextflow",
            "bcftools_binary": dummy_binary,
            "vcf_validator_binary": dummy_binary,
            "assembly_checker_binary": dummy_binary,
            "fasta_path": os.path.join(self.resources_folder, "GCA_009858895.3_ASM985889v3_genomic.fna"),
            "assembly_report_path": os.path.join(self.resources_folder,
                                                 "GCA_009858895.3_ASM985889v3_assembly_report.txt")
        }
        with open(self.config_file, "w") as config_file_handle:
            yaml.safe_dump(app_config, config_file_handle)

    def tearDown(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_run_ingest_covid19_submission(self):
        toplevel_vcf_dir = os.path.join(self.resources_folder, "vcf_files")
        ingest_covid19_submission(toplevel_vcf_dir, None, self.config_file, resume=False)