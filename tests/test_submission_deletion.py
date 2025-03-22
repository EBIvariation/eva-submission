import os
import shutil
import tarfile
from pathlib import Path
from unittest import TestCase

import yaml
from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_deletion import EloadDeletion
from eva_submission.submission_config import load_config


class TestEloadDeletion(TestCase):
    test_top_dir = os.path.dirname(__file__)
    sub_del_test_dir = os.path.join(test_top_dir, 'test_submission_deletion')

    def setUp(self) -> None:
        if os.path.exists(self.sub_del_test_dir):
            shutil.rmtree(self.sub_del_test_dir)
        os.makedirs(self.sub_del_test_dir, exist_ok=True)

        # create and load config file
        config_file = self.create_config_file(self.sub_del_test_dir)
        load_config(config_file)

        # create eload config
        eload_number = 1
        project_acc = "PRJEB11111"

        # create eloads directory and config file
        eload_config_dir = os.path.join(cfg['eloads_dir'], f"ELOAD_{eload_number}")
        os.makedirs(eload_config_dir, exist_ok=True)
        self.create_eload_config_file(eload_config_dir, eload_number, project_acc)

        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.sub_del_test_dir)
        self.eload_deletion = EloadDeletion(eload_number)

    def tearDown(self):
        if os.path.exists(self.sub_del_test_dir):
            shutil.rmtree(self.sub_del_test_dir)

    def create_config_file(self, config_file_dir):
        config_file_path = os.path.join(config_file_dir, 'config.yml')
        # remove file if already exists
        if os.path.exists(config_file_path):
            os.remove(config_file_path)

        data = {"eloads_dir": os.path.join(self.sub_del_test_dir, "eloads"),
                "projects_dir": os.path.join(self.sub_del_test_dir, "projects"),
                "ftp_dir": os.path.join(self.sub_del_test_dir, "ftp"),
                "eloads_lts_dir": os.path.join(self.sub_del_test_dir, "lts")}
        with open(config_file_path, "w") as file:
            yaml.dump(data, file, default_flow_style=False)

        return config_file_path

    def create_eload_config_file(self, config_file_dir, eload_number, project_accession):
        config_file_path = os.path.join(config_file_dir, f'.ELOAD_{eload_number}_config.yml')
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
        self.eload_deletion.delete_project_dir(project_dir)
        # assert
        assert not os.path.exists(project_dir)

    def test_delete_eload_dir(self):
        # create required directory and files
        eload_dir = os.path.join(self.sub_del_test_dir, 'eload_dir', 'ELOAD_1')
        os.makedirs(eload_dir)
        Path(f'{eload_dir}/test1.txt').touch()
        Path(f'{eload_dir}/test2.txt').touch()
        # call method
        self.eload_deletion.delete_eload_dir(eload_dir)
        # assert
        assert not os.path.exists(eload_dir)

    def test_delete_ftp_dir(self):
        # create required directory and files
        ftp_dir = os.path.join(self.sub_del_test_dir, 'ftp_dir', 'eva-box-01')
        os.makedirs(ftp_dir)
        Path(f'{ftp_dir}/test1.txt').touch()
        Path(f'{ftp_dir}/test2.txt').touch()
        # call method
        self.eload_deletion.delete_ftp_dir(ftp_dir)
        # assert
        assert not os.path.exists(ftp_dir)

    def test_copy_eload_files(self):
        # setup data
        self.setup_test_eload_data(1)

        # call method
        archive_dir = os.path.join(self.sub_del_test_dir, 'archive_dir')
        os.makedirs(archive_dir)
        self.eload_deletion.copy_eload_files(archive_dir)

        # assert
        assert os.path.exists(os.path.join(archive_dir, '.ELOAD_1_config.yml'))
        assert os.path.exists(os.path.join(archive_dir, 'ELOAD_1_submission.log'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'metadata_spreadsheet.xlsx'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(archive_dir, '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(archive_dir, '00_logs', 'test_log_1.txt'))
        assert os.path.exists(os.path.join(archive_dir, '00_logs', 'test_log_2.txt'))
        assert os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_1.accessioned.vcf.gz'))

        assert not os.path.exists(os.path.join(archive_dir, '60_eva_public', 'test_2.accessioned.vcf'))

    def test_delete_submission(self):
        # setup test data
        self.setup_test_eload_data(1)
        self.setup_test_ftp_boxes_data(1, 'test_user')

        # call method
        self.eload_deletion.delete_submission(1, 'test_user')

        # assert ftp files are deleted
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload'))
        assert not os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload', 'test_user'))
        # assert eload dir is deleted
        assert not os.path.exists(os.path.join(self.eload_deletion.eload_dir))
        # assert project dir is deleted
        assert not os.path.exists(os.path.join(self.eload_deletion.project_dir))

        # extract archived tar file
        src_tar_file = os.path.join(cfg['eloads_lts_dir'], f"{self.eload_deletion.eload}.tar")
        target_tar_file = os.path.join(self.sub_del_test_dir, f"{self.eload_deletion.eload}")
        if os.path.exists(target_tar_file):
            os.remove(target_tar_file)

        with tarfile.open(src_tar_file, "r:*") as tar:
            tar.extractall(path=target_tar_file)

        # assert file copied
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '.ELOAD_1_config.yml'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', 'ELOAD_1_submission.log'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'metadata_spreadsheet.xlsx'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '00_logs', 'test_log_1.txt'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '00_logs', 'test_log_2.txt'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '60_eva_public', 'test_1.accessioned.vcf.gz'))
        # assert file not copied
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '60_eva_public', 'test_2.accessioned.vcf'))

    def test_delete_submission_with_old_version(self):
        # setup test data
        self.setup_test_eload_data(1, old_version=True)
        self.setup_test_ftp_boxes_data(1, 'test_user')

        # set config values for old_version
        self.eload_deletion.eload_cfg.set('version', value='1.15')
        self.eload_deletion.eload_cfg.set('ingestion', 'project_dir', value=self.eload_deletion.project_dir)

        # call method
        self.eload_deletion.delete_submission(1, 'test_user')

        # assert ftp files are deleted
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload'))
        assert not os.path.exists(os.path.join(self.sub_del_test_dir, 'ftp', 'eva-box-01', 'upload', 'test_user'))
        # assert eload dir is deleted
        assert not os.path.exists(os.path.join(self.eload_deletion.eload_dir))
        # assert project dir is deleted
        assert not os.path.exists(os.path.join(self.eload_deletion.project_dir))

        # extract archived tar file
        src_tar_file = os.path.join(cfg['eloads_lts_dir'], f"{self.eload_deletion.eload}.tar")
        target_tar_file = os.path.join(self.sub_del_test_dir, f"{self.eload_deletion.eload}")
        if os.path.exists(target_tar_file):
            os.remove(target_tar_file)

        with tarfile.open(src_tar_file, "r:*") as tar:
            tar.extractall(path=target_tar_file)

        # assert file copied
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '.ELOAD_1_config.yml'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', 'ELOAD_1_submission.log'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'metadata_spreadsheet.xlsx'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'test_1.vcf.gz'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '18_brokering', 'ena', 'test_1.vcf.csi'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '00_logs', 'test_log_1.txt'))
        assert os.path.exists(os.path.join(self.sub_del_test_dir, 'ELOAD_1', '00_logs', 'test_log_2.txt'))
        assert os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '60_eva_public', 'test_1.accessioned.vcf.gz'))
        # assert file not copied
        assert not os.path.exists(
            os.path.join(self.sub_del_test_dir, 'ELOAD_1', '60_eva_public', 'test_2.accessioned.vcf'))

    def setup_test_ftp_boxes_data(self, ftp_box, submitter):
        ftp_box_path = os.path.join(cfg['ftp_dir'], 'eva-box-%02d' % ftp_box, 'upload', submitter)
        os.makedirs(ftp_box_path, exist_ok=True)
        Path(f"{ftp_box_path}/test_1.vcf").touch()

    def setup_test_eload_data(self, eload_number, old_version=False):
        # create eload submission logs
        Path(f"{self.eload_deletion.eload_dir}/ELOAD_{eload_number}_submission.log").touch()

        # create metadata spreadsheet
        Path(f"{self.eload_deletion.eload_dir}/18_brokering/ena/metadata_spreadsheet.xlsx").touch()

        # create eload vcf files
        Path(f"{self.eload_deletion.eload_dir}/18_brokering/ena/test_1.vcf.gz").touch()
        Path(f"{self.eload_deletion.eload_dir}/18_brokering/ena/test_1.vcf.csi").touch()

        prj_eload_dir = self.eload_deletion.eload_dir
        if old_version:
            prj_eload_dir = self.eload_deletion.project_dir

        # create 00_log dir and files
        log_dir = os.path.join(prj_eload_dir, '00_logs')
        os.makedirs(log_dir, exist_ok=True)
        Path(f"{log_dir}/test_log_1.txt").touch()
        Path(f"{log_dir}/test_log_2.txt").touch()

        # create 60_eva_public dir and accessioned files
        accessioned_files_dir = os.path.join(prj_eload_dir, '60_eva_public')
        os.makedirs(accessioned_files_dir, exist_ok=True)
        Path(f"{accessioned_files_dir}/test_1.accessioned.vcf.gz").touch()
        Path(f"{accessioned_files_dir}/test_2.accessioned.vcf").touch()

        # create lts directory
        os.makedirs(cfg['eloads_lts_dir'])
