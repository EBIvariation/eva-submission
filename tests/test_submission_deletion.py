import os
import shutil
import tarfile
from pathlib import Path
from unittest import TestCase

import yaml
from ebi_eva_common_pyutils.config import cfg
from eva_submission.submission_qc_checks import SubmissionQC

from eva_submission.submission_deletion import SubmissionDeletion
from eva_submission.submission_config import load_config


class TestSubmissionDeletion(TestCase):
    test_top_dir = os.path.dirname(__file__)
    sub_del_test_dir = os.path.join(test_top_dir, 'test_submission_deletion')

    def setUp(self) -> None:
        if os.path.exists(self.sub_del_test_dir):
            shutil.rmtree(self.sub_del_test_dir)
        os.makedirs(self.sub_del_test_dir, exist_ok=True)

        # create and load config file
        config_file = self.create_config_file(self.sub_del_test_dir)
        load_config(config_file)

        # create submission config
        submission_id = 'submission_1'
        project_acc = "PRJEB11111"

        # create submissions directory and config file
        submission_config_dir = os.path.join(cfg['submissions_dir'], f"{submission_id}")
        os.makedirs(submission_config_dir, exist_ok=True)
        self.create_submission_config_file(submission_config_dir, submission_id, project_acc)

        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.sub_del_test_dir)
        # create lts directory
        os.makedirs(cfg['submissions_lts_dir'], exist_ok=True)
        self.submission_deletion = SubmissionDeletion(submission_id)

    def tearDown(self):
        if os.path.exists(self.sub_del_test_dir):
            shutil.rmtree(self.sub_del_test_dir)

    def create_config_file(self, config_file_dir):
        config_file_path = os.path.join(config_file_dir, 'config.yml')
        # remove file if already exists
        if os.path.exists(config_file_path):
            os.remove(config_file_path)

        data = {"eloads_dir": os.path.join(self.sub_del_test_dir, "eloads"),
                "submissions_dir": os.path.join(self.sub_del_test_dir, "eloads"),
                "nobackup_eloads_dir": os.path.join(self.sub_del_test_dir, "nobackup_eloads"),
                "nobackup_submissions_dir": os.path.join(self.sub_del_test_dir, "nobackup_eloads"),
                "projects_dir": os.path.join(self.sub_del_test_dir, "projects"),
                "ftp_dir": os.path.join(self.sub_del_test_dir, "ftp"),
                "eloads_lts_dir": os.path.join(self.sub_del_test_dir, "lts"),
                "submissions_lts_dir": os.path.join(self.sub_del_test_dir, "lts")}
        with open(config_file_path, "w") as file:
            yaml.dump(data, file, default_flow_style=False)

        return config_file_path

    def create_submission_config_file(self, config_file_dir, submission_id, project_accession):
        config_file_path = os.path.join(config_file_dir, f'.{submission_id}_config.yml')
        # remove file if already exists
        if os.path.exists(config_file_path):
            os.remove(config_file_path)

        data = {"brokering": {"ena": {"PROJECT": project_accession}}, "version": '1.16'}
        with open(config_file_path, "w") as file:
            yaml.dump(data, file, default_flow_style=False)

    def test_delete_project_dir(self):
        # create required directory and files
        project_dir = os.path.join(self.sub_del_test_dir, 'project_dir', 'PRJEB11111')
        os.makedirs(project_dir)
        Path(f'{project_dir}/test1.txt').touch()
        Path(f'{project_dir}/test2.txt').touch()
        # call delete method
        self.submission_deletion.delete_project_dir(project_dir)
        # assert
        assert not os.path.exists(project_dir)

    def test_delete_submission_dir(self):
        # create required directory and files
        submission_dir = os.path.join(self.sub_del_test_dir, 'eload_dir', 'submission_1')
        os.makedirs(submission_dir)
        Path(f'{submission_dir}/test1.txt').touch()
        Path(f'{submission_dir}/test2.txt').touch()
        # call method
        self.submission_deletion.delete_submission_dir(submission_dir)
        # assert
        assert not os.path.exists(submission_dir)

    def test_delete_ftp_dir(self):
        # create required directory and files
        ftp_dir = os.path.join(self.sub_del_test_dir, 'ftp_dir', 'eva-box-01')
        os.makedirs(ftp_dir)
        Path(f'{ftp_dir}/test1.txt').touch()
        Path(f'{ftp_dir}/test2.txt').touch()
        # call method
        self.submission_deletion.delete_ftp_dir(ftp_dir)
        # assert
        assert not os.path.exists(ftp_dir)

    def test_copy_submission_files(self):
        # setup data
        self.setup_test_submission_data(1)

        # call method
        archive_dir = os.path.join(self.sub_del_test_dir, 'archive_dir')
        os.makedirs(archive_dir)
        self.submission_deletion.copy_submission_files(archive_dir)

        # assert
        assert os.path.exists(os.path.join(archive_dir, '.submission_1_config.yml'))
        assert os.path.exists(os.path.join(archive_dir, 'submission_1_submission.log'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'metadata_spreadsheet.xlsx'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'test_1.vcf.gz.csi'))
        assert os.path.exists(os.path.join(archive_dir, '00_logs', 'test_log_1.txt'))
        assert os.path.exists(os.path.join(archive_dir, '00_logs', 'test_log_2.txt'))
        assert os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_1.accessioned.vcf.gz'))
        assert os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_1.accessioned.vcf.gz.csi'))

        assert not os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_2.accessioned.vcf'))
        assert not os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_2.accessioned.vcf.csi'))

    def test_delete_submission_already_existing_lts_no_force_delete(self):
        # create existing lts file
        os.makedirs(cfg['submissions_lts_dir'], exist_ok=True)
        Path(f"{cfg['submissions_lts_dir']}/submission_1.tar/").touch()

        with self.assertRaises(Exception) as context:
            self.submission_deletion.delete_submission(1, 'test_user')
        self.assertIn("File already exists in the LTS", str(context.exception))

    def test_delete_submission_failed_qc_no_force_delete(self):
        self.setup_test_submission_data(1)
        self.setup_test_ftp_boxes_data(1, 'test_user')

        # No QC in config
        with self.assertRaises(Exception) as context:
            self.submission_deletion.delete_submission(1, 'test_user')
        self.assertIn('QC has not been run successfully', str(context.exception))

        # Failed QC in config
        self.submission_deletion.submission_cfg.set(SubmissionQC.config_section, value={
            'accessioning': 'FAIL',
            'variants_skipped_accessioning': 'FAIL',
            'variant_load': 'PASS',
            'annotation': 'PASS'
        })
        with self.assertRaises(Exception) as context:
            self.submission_deletion.delete_submission(1, 'test_user')
        self.assertIn('QC has not been run successfully', str(context.exception))

    def test_delete_submission(self):
        # setup test data
        self.setup_test_submission_data('submission_1')
        self.setup_test_ftp_boxes_data(1, 'test_user')
        # Successful QC in config
        self.submission_deletion.submission_cfg.set(SubmissionQC.config_section, value={
            'accessioning': 'PASS',
            'variants_skipped_accessioning': 'PASS with Warning (Manual Check Required)',
            'variant_load': 'PASS',
            'annotation': 'SKIP'
        })

        # call method
        self.submission_deletion.delete_submission(1, 'test_user')

        # assert ftp files are deleted
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload'))
        assert not os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload', 'test_user'))
        # assert submission dir is deleted
        assert not os.path.exists(os.path.join(self.submission_deletion.submission_dir))
        # assert project dir is deleted
        assert not os.path.exists(os.path.join(self.submission_deletion.project_dir))

        # extract archived tar file
        src_tar_file = os.path.join(cfg['submissions_lts_dir'], f"{self.submission_deletion.submission_id}.tar")


        with tarfile.open(src_tar_file, "r:*") as tar:
            tar.extractall(path=self.sub_del_test_dir)
        # assert file copied
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '.submission_1_config.yml.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', 'submission_1_submission.log.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'metadata_spreadsheet.xlsx.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.gz.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '00_logs', 'test_log_1.txt.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '00_logs', 'test_log_2.txt.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_1.accessioned.vcf.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_1.accessioned.vcf.gz.csi'))
        # assert file not copied
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_2.accessioned.vcf.gz'))
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_2.accessioned.vcf.csi'))

    def test_delete_submission_with_old_version(self):
        # setup test data
        self.setup_test_submission_data('submission_1', old_version=True)
        self.setup_test_ftp_boxes_data(1, 'test_user')

        # create existing lts file
        Path(f"{cfg['submissions_lts_dir']}/submission_1.tar/").touch()

        # set config values for old_version
        self.submission_deletion.submission_cfg.set('version', value='1.15')
        self.submission_deletion.submission_cfg.set('ingestion', 'project_dir', value=self.submission_deletion.project_dir)

        # call method
        self.submission_deletion.delete_submission(1, 'test_user', force_delete=True)

        # assert ftp files are deleted
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload'))
        assert not os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload', 'test_user'))
        # assert submission dir is deleted
        assert not os.path.exists(os.path.join(self.submission_deletion.submission_dir))
        # assert project dir is deleted
        assert not os.path.exists(os.path.join(self.submission_deletion.project_dir))

        # extract archived tar file
        src_tar_file = os.path.join(cfg['submissions_lts_dir'], f"{self.submission_deletion.submission_id}.tar")

        with tarfile.open(src_tar_file, "r:*") as tar:
            tar.extractall(path=self.sub_del_test_dir)

        # assert file copied
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '.submission_1_config.yml.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', 'submission_1_submission.log.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'metadata_spreadsheet.xlsx.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '18_brokering', 'ena', 'test_1.vcf.gz.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '00_logs', 'test_log_1.txt.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'submission_1', '00_logs', 'test_log_2.txt.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_1.accessioned.vcf.gz'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_1.accessioned.vcf.gz.csi'))
        # assert file not copied
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_2.accessioned.vcf.gz'))
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'submission_1', '60_eva_public', 'test_2.accessioned.vcf.csi'))

    def setup_test_ftp_boxes_data(self, ftp_box, submitter):
        ftp_box_path = os.path.join(cfg['ftp_dir'], 'eva-box-%02d' % ftp_box, 'upload', submitter)
        os.makedirs(ftp_box_path, exist_ok=True)
        Path(f"{ftp_box_path}/test_1.vcf").touch()

    def setup_test_submission_data(self, submission_id, old_version=False):
        # create submission submission logs
        Path(f"{self.submission_deletion.submission_dir}/{submission_id}_submission.log").touch()

        # create metadata spreadsheet
        Path(f"{self.submission_deletion.submission_dir}/18_brokering/ena/metadata_spreadsheet.xlsx").touch()

        # create submission vcf files
        Path(f"{self.submission_deletion.submission_dir}/18_brokering/ena/test_1.vcf.gz").touch()
        Path(f"{self.submission_deletion.submission_dir}/18_brokering/ena/test_1.vcf.csi").touch()
        Path(f"{self.submission_deletion.submission_dir}/18_brokering/ena/test_1.vcf.gz.csi").touch()

        prj_eload_dir = self.submission_deletion.submission_dir
        if old_version:
            prj_eload_dir = self.submission_deletion.project_dir

        # create 00_log dir and files
        log_dir = os.path.join(prj_eload_dir, '00_logs')
        os.makedirs(log_dir, exist_ok=True)
        Path(f"{log_dir}/test_log_1.txt").touch()
        Path(f"{log_dir}/test_log_2.txt").touch()

        # create 60_eva_public dir and accessioned files
        accessioned_files_dir = os.path.join(prj_eload_dir, '60_eva_public')
        os.makedirs(accessioned_files_dir, exist_ok=True)
        Path(f"{accessioned_files_dir}/test_1.accessioned.vcf.gz").touch()
        Path(f"{accessioned_files_dir}/test_1.accessioned.vcf.gz.csi").touch()
        Path(f"{accessioned_files_dir}/test_2.accessioned.vcf").touch()
        Path(f"{accessioned_files_dir}/test_2.accessioned.vcf.csi").touch()

